package com.hhzone.rlogex.filereader;

import static java.text.MessageFormat.format;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.RecordParser;

public class FileReaderVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(FileReaderVerticle.class);

    private static final String LOGS_GROUP_SIZE = "logs.lines.group.size";
    private static final int LOGS_GROUP_SIZE_DEFAULT = 1000;
    private static final String LOGS_READ_POOL_SIZE = "logs.read.pool.size";
    private static final int LOGS_READ_POOL_SIZE_DEFAULT = 1;
    private static final String LOGS_DEFAULT_TIMEZONE = "logs.default.timezone";
    private static final String LOGS_DEFAULT_TIMEZONE_DEFAULT = "Europe/London";

    private static final String FILE_NAME_PROPERTY_NAME_FORMAT = "logs.{0}.fileName";
    private static final String LINE_PROPERTY_NAME_FORMAT = "logs.{0}.linePattern";
    private static final String TIMESTAMP_PROPERTY_NAME_FORMAT = "logs.{0}.timestampFormat";

    private MessageConsumer<JsonObject> messageConsumer;
    private int groupSize;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        this.groupSize = config().getInteger(LOGS_GROUP_SIZE, LOGS_GROUP_SIZE_DEFAULT);
        messageConsumer = vertx.eventBus().consumer("LOGS", this::read);
        startFuture.complete();
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        messageConsumer.unregister();
        stopFuture.complete();
    }

    private void read(Message<JsonObject> message) {
        final JsonObject request = message.body();
        final LogType type = LogType.valueOf(request.getString("type"));
        final String node = request.getString("node");
        final Date forDate = Date.from(request.getInstant("date"));
        final String fileName = format(config().getString(format(FILE_NAME_PROPERTY_NAME_FORMAT, type.getName())), node, forDate);
        logger.info("File {0} reading...", fileName);
        final Pattern linePattern = Pattern.compile(config().getString(format(LINE_PROPERTY_NAME_FORMAT, type.getName())));
        final SimpleDateFormat timestampFormat = new SimpleDateFormat(config().getString(format(TIMESTAMP_PROPERTY_NAME_FORMAT, type.getName())));
        timestampFormat.setTimeZone(TimeZone.getTimeZone(config().getString(LOGS_DEFAULT_TIMEZONE, LOGS_DEFAULT_TIMEZONE_DEFAULT)));
        try {
            final LinesHandler handler = new LinesHandler(fileName, type, forDate, node, timestampFormat, linePattern,
                    this::send,
                    v -> {
                        logger.info("File {0} is read", fileName);
                        message.reply(new JsonObject().put("lines", v));
                    },
                    groupSize
            );
            final RecordParser recordHandler = RecordParser.newDelimited("\n", handler);
            new AsyncInputStream(vertx, new GZIPInputStream(new FileInputStream(fileName)), config().getInteger(LOGS_READ_POOL_SIZE, LOGS_READ_POOL_SIZE_DEFAULT))
                    .handler(recordHandler).endHandler(handler.endHandler());
        }
        catch (FileNotFoundException e) {
            logger.warn("File {0} not found", fileName, e);
            message.fail(1, e.getMessage());
        }
        catch (Exception e) {
            logger.error("Error processing file {0}", fileName, e);
            message.fail(2, e.getMessage());
        }
    }

    private void send(JsonObject message) {
        vertx.eventBus().send("INDEX_WRITE", message);
    }
}
