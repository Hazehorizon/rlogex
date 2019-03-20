package com.hhzone.rlogex.provider;

import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toMap;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import com.hhzone.rlogex.PersistentProperties;
import com.hhzone.rlogex.messages.ValueGetMessageBuilder;
import com.hhzone.rlogex.messages.ValueRemoveMessageBuilder;
import com.hhzone.rlogex.messages.ValueSetMessageBuilder;
import com.hhzone.rlogex.storage.ValueType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.templ.TemplateEngine;

public class AdminHandler extends AbstractHttpHandler {
    private static final SimpleDateFormat LAST_READED_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    public AdminHandler(TemplateEngine templateEngine, EventBus eventBus) {
        super(templateEngine, eventBus);
    }

    public void admin(RoutingContext context) {
        getEventBus().send("STORAGE", new ValueGetMessageBuilder(ValueType.DATE, PersistentProperties.LAST_READED_PROPERTY_NAME, null).build(), (AsyncResult<Message<JsonObject>> r) -> {
            if (r.succeeded()) {
                if (nonNull(ValueType.DATE.getter().apply(r.result().body()))) {
                    context.put("lastReadedDate", LAST_READED_DATE_FORMAT.format(ValueType.DATE.<Date>getter().apply(r.result().body())));
                }
                getEventBus().send("STORAGE", new ValueGetMessageBuilder(ValueType.LONG, PersistentProperties.DATE_NODE_LINE_PROPERTY_NAME).build(), (AsyncResult<Message<JsonObject>> r1) -> {
                    if (r1.succeeded()) {
                        render("/admin.ftl", context.put("files", toViewModel(r1.result().body())));
                    }
                    else {
                        context.fail(r.cause());
                    }
                });
            }
            else {
                context.fail(r.cause());
            }
        });
    }

    public void removeFileDate(RoutingContext context) {
        getEventBus().send("STORAGE", new ValueRemoveMessageBuilder(PersistentProperties.DATE_NODE_LINE_PROPERTY_NAME, context.request().getParam("key").split("_")).build(), reroutHandler(context));        
    }

    public void reloadDate(RoutingContext context) {
        try {
            final Date date = PersistentProperties.DATE_KEY_FORMAT.parse(context.request().getParam("date"));
            getEventBus().send("SCHEDULER", new JsonObject().put("date", date.toInstant()));
            context.reroute("/admin");
        } catch (ParseException e) {
            context.fail(e);
        }
    }

    public void resetDate(RoutingContext context) {
        getEventBus().send("STORAGE", new ValueRemoveMessageBuilder(PersistentProperties.LAST_READED_PROPERTY_NAME, null).build(), reroutHandler(context));        
    }

    public void setDate(RoutingContext context) {
        try {
            final Date newDate = LAST_READED_DATE_FORMAT.parse(context.request().getParam("lastReadedDate"));
            getEventBus().send("STORAGE", new ValueSetMessageBuilder(ValueType.DATE, PersistentProperties.LAST_READED_PROPERTY_NAME, newDate, null).build(), reroutHandler(context));
        } catch (ParseException e) {
            context.fail(e);
        }
    }

    private JsonObject toViewModel(JsonObject object) {
        final Map<String, Map<String, Map<String, Long>>> byKeys = new TreeMap<>();
        object.getJsonArray("values").stream().map(o -> (JsonObject)o).forEach(j -> byKeys
                .computeIfAbsent(j.getJsonArray("keys").getString(0), k -> new TreeMap<>())
                    .computeIfAbsent(j.getJsonArray("keys").getString(1), k -> new TreeMap<>())
                        .put(j.getJsonArray("keys").getString(2), j.getLong("value")));

        return new JsonObject(byKeys.entrySet().stream().collect(toMap(Map.Entry::getKey, e0 -> new JsonObject(
            e0.getValue().entrySet().stream().collect(toMap(Map.Entry::getKey, e1 -> new JsonObject((Map)e1.getValue()), throwingMerger(), TreeMap::new))), throwingMerger(), TreeMap::new)));
    }

    private Handler<AsyncResult<Message<JsonObject>>> reroutHandler(RoutingContext context) {
        return r -> {
            if (r.succeeded()) {
                context.reroute("/admin");
            }
            else {
                context.fail(r.cause());
            }
        };
    }

    private static <T> BinaryOperator<T> throwingMerger() {
        return (u,v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); };
    }
}
