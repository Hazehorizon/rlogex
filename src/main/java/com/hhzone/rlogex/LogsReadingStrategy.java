package com.hhzone.rlogex;

import static java.util.Objects.isNull;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.hhzone.rlogex.filereader.LogType;
import com.hhzone.rlogex.messages.ReadLogFileMessage;
import com.hhzone.rlogex.messages.ValueGetMessageBuilder;
import com.hhzone.rlogex.messages.ValueSetMessageBuilder;
import com.hhzone.rlogex.storage.ValueType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class LogsReadingStrategy {
    private static final String LOGS_NODES = "logs.nodes";
    private static final String LOGS_READ_TIMEOUT = "logs.file.read.timeout";
    private static final String LOGS_READ_TIMEOUT_DEFAULT = "PT5M";
    private static final String LOGS_FILE_RELOAD_TIMEOUT = "logs.file.reload.timeout";
    private static final String LOGS_FILE_RELOAD_TIMEOUT_DEFAULT = "PT2M";

    private final Date date;
    private final String dateAsString;
    private final List<String> nodes;
    private final Vertx vertx;
    private final Long readTimeout;
    private final Long fileReloadTimeout;

    public LogsReadingStrategy(Date date, Vertx vertx, JsonObject config) {
        this.date = date;
        this.vertx = vertx;
        this.nodes = config.getJsonArray(LOGS_NODES).getList();
        this.readTimeout = Duration.parse(config.getString(LOGS_READ_TIMEOUT, LOGS_READ_TIMEOUT_DEFAULT)).toMillis();
        this.fileReloadTimeout = Duration.parse(config.getString(LOGS_FILE_RELOAD_TIMEOUT, LOGS_FILE_RELOAD_TIMEOUT_DEFAULT)).toMillis();
        this.dateAsString = PersistentProperties.DATE_KEY_FORMAT.format(date);
    }

    public void start(Handler<AsyncResult<Void>> onComplete) {
        vertx.eventBus().send("STORAGE", new ValueGetMessageBuilder(ValueType.LONG, PersistentProperties.DATE_NODE_LINE_PROPERTY_NAME, dateAsString).build(), (AsyncResult<Message<JsonObject>> r) -> {
            if (r.succeeded()) {
                final Map<List<String>, Long> readed = toReadedMap(r.result().body());
                boolean notFound = true;
                for (Iterator<String> nItr = nodes.iterator(); notFound && nItr.hasNext();) {
                    final String node = nItr.next();
                    for(Iterator<LogType> tItr = Arrays.asList(LogType.values()).iterator(); notFound && tItr.hasNext();) {
                        final LogType type = tItr.next();
                        final Long lines = readed.get(Arrays.asList(dateAsString, type.name(), node));
                        if (isNull(lines)) {
                            notFound = false;
                            send(node, type, onComplete, r1 -> saveLinesCount(node, type.name(), r1, r2 -> {
                                    if (r2.succeeded()) {
                                        proceedAgain(onComplete);
                                    }
                                    else {
                                        onComplete.handle(Future.failedFuture(r.cause()));
                                    }
                                }));
                        }
                    }
                }
                if (notFound) {
                    onComplete.handle(Future.succeededFuture());
                }
            }
            else {
                onComplete.handle(Future.failedFuture(r.cause()));
            }
        });
    }

    private void proceedAgain(Handler<AsyncResult<Void>> onComplete) {
        vertx.setTimer(fileReloadTimeout, t -> start(onComplete));
    }

    private void send(String node, LogType type, Handler<AsyncResult<Void>> onError, Handler<Long> onLoaded) {
        final DeliveryOptions options = new DeliveryOptions().setSendTimeout(readTimeout);
        vertx.eventBus().send("LOGS", new ReadLogFileMessage(node, date, type).build(), options, (AsyncResult<Message<JsonObject>> r) -> {
            if (r.succeeded()) {
                onLoaded.handle(r.result().body().getLong("lines"));
            }
            else {
                if (r.cause() instanceof ReplyException && ((ReplyException)r.cause()).failureCode() == 1) {
                    onLoaded.handle(0L);
                }
                else {
                    onError.handle(Future.failedFuture(r.cause()));
                }
            }
        });
    }

    private void saveLinesCount(String node, String type, Long lines, Handler<AsyncResult<Message<Void>>> replyHandler) {
        vertx.eventBus().send("STORAGE", new ValueSetMessageBuilder(ValueType.LONG, PersistentProperties.DATE_NODE_LINE_PROPERTY_NAME, lines, dateAsString, type, node).build(), replyHandler);
    }

    private static Map<List<String>, Long> toReadedMap(JsonObject object) {
        final Map<List<String>, Long> map = new HashMap<>();
        final JsonArray logs = object.getJsonArray("values");
        for(int i=0; i<logs.size(); ++i) {
            final JsonObject log = logs.getJsonObject(i);
            map.put(log.getJsonArray("keys").getList(), log.getLong("value"));
        }
        return map;
    }
}
