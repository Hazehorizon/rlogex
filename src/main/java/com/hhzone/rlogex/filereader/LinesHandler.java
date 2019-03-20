package com.hhzone.rlogex.filereader;

import static java.util.Objects.nonNull;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class LinesHandler implements Handler<Buffer> {
    private final String fileName;
    private final LogType type;
    private final Date date;
    private final String node;
    private final SimpleDateFormat timestampFormat;
    private final Pattern linePattern;

    private final Handler<JsonObject> handler;
    private final Handler<Long> endHandler;

    private final int groupSize;
    private JsonArray group;

    private Matcher lastMatch;
    private String lastLine;
    private long lineNumber;

    public LinesHandler(String fileName, LogType type, Date date, String node,
            SimpleDateFormat timestampFormat, Pattern linePattern,
            Handler<JsonObject> handler, Handler<Long> endHandler, int groupSize) {
        this.fileName = fileName;;
        this.type = type;
        this.date = date;
        this.node = node;
        this.timestampFormat = timestampFormat;
        this.linePattern = linePattern;

        this.handler = handler;
        this.endHandler = endHandler;

        this.groupSize = groupSize;
        this.group = new JsonArray();
    }

    @Override
    public void handle(Buffer buffer) {
        final String line = buffer.toString();
        final Matcher matcher = linePattern.matcher(line);
        if (matcher.matches()) {
            if (nonNull(lastLine) && nonNull(lastMatch)) {
                handleLine(new LineContext(fileName, type, timestampFormat, date, node, ++lineNumber, lastLine, lastMatch));
            }
            lastLine = line;
            lastMatch = matcher;
        }
        else {
            lastLine += "\n" + line;
        }
    }

    public Handler<Void> endHandler() {
        return r -> {
            handleLine(new LineContext(fileName, type, timestampFormat, date, node, ++lineNumber, lastLine, lastMatch));
            sendGroup();
            endHandler.handle(lineNumber);
        };
    }

    private void handleLine(LineContext line) {
        final JsonObject json = line.getType().getBuilder().apply(line);
        group.add(json);
        if (group.size() >= groupSize) {
            sendGroup();
        }
    }

    private void sendGroup() {
        if (group.size() > 0) {
            handler.handle(new JsonObject().put("lines", group));
            group = new JsonArray();
        }
    }
}
