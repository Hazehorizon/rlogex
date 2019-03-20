package com.hhzone.rlogex.filereader;
import static java.util.stream.Collectors.toSet;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.vertx.core.json.JsonObject;

public class JsonObjectBuilders {
    private static final String ID_FORMAT = "{0,number,0}{1}{2,number,00000}{3,number,0000000}";
    private static final Calendar TIME_START;

    static {
        TIME_START = Calendar.getInstance();
        TIME_START.set(2000, 0, 1);
    }

    public static JsonObject buildAccessLineMessage(LineContext context) {
        final Date endTime = parseDate(context.getTimestampFormat(), context.getMatcher().group("endTime"));
        final long time = Long.parseLong(context.getMatcher().group("time"));
        final Date startTime = new Date(endTime.getTime() - time);
        final JsonObject message = createFromMatcher(context.getLine(), context.getMatcher(), "endTime", "time");
        fillFromContext(message, context);
        message.put("timestamp", startTime.toInstant());
        message.put("endTime", endTime.toInstant());
        message.put("duration", time);
        return message;
    }

    public static JsonObject buildLineMessage(LineContext context) {
        final Date timestamp = parseDate(context.getTimestampFormat(), context.getMatcher().group("timestamp"));
        final JsonObject message = createFromMatcher(context.getLine(), context.getMatcher(), "timestamp");
        fillFromContext(message, context);
        message.put("timestamp", timestamp.toInstant());
        return message;
    }

    public static JsonObject buildDeviceLineMessage(LineContext context) {
        final Date timestamp = parseDate(context.getTimestampFormat(), context.getMatcher().group("timestamp"));
        final JsonObject message = createFromMatcher(context.getLine().replaceAll("(\\\\n|\\|)", "\n"), context.getMatcher(), "timestamp");
        fillFromContext(message, context);
        message.put("timestamp", timestamp.toInstant());
        return message;
    }

    private static void fillFromContext(JsonObject message, LineContext context) {
        message.put("id", createId(context));
        message.put("fileName", context.getFileName());
        message.put("lineNo", context.getLineNumber());
        message.put("type", context.getType().name());
        message.put("node", context.getNode());        
    }

    private static long createId(LineContext context) {
        return Long.parseLong(MessageFormat.format(ID_FORMAT,
                context.getType().ordinal() + 1,
                context.getNode().substring(context.getNode().length() - 3),
                ChronoUnit.DAYS.between(TIME_START.toInstant(), context.getDate().toInstant()),
                context.getLineNumber()));
    }

    private static JsonObject createFromMatcher(String line, Matcher matcher, String... excludedGroups) {
        final Set<String> exclusions = Arrays.stream(excludedGroups).collect(toSet());
        final JsonObject message = new JsonObject();
        getNamedGroups(matcher.pattern()).entrySet().stream().filter(e -> !exclusions.contains(e.getKey())).forEach(e -> message.put(e.getKey(), matcher.group(e.getValue())));
        message.put("line", line);
        return message;
    }

    private static Map<String, Integer> getNamedGroups(Pattern regex) {
        try {
            Method namedGroupsMethod = Pattern.class.getDeclaredMethod("namedGroups");
            namedGroupsMethod.setAccessible(true);
            return (Map<String, Integer>)namedGroupsMethod.invoke(regex);
        }
        catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static Date parseDate(SimpleDateFormat format, String source) {
        try {
            return format.parse(source);
        } catch (ParseException e) {
            throw new TimestampParsingException(e);
        }
    }

    public static class TimestampParsingException extends RuntimeException {
        public TimestampParsingException(Throwable cause) {
            super(cause);
        }
    }
}
