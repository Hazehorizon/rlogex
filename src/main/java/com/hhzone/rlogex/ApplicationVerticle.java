package com.hhzone.rlogex;

import static java.util.Objects.nonNull;

import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import com.hhzone.rlogex.filereader.FileReaderVerticle;
import com.hhzone.rlogex.index.IndexSearchVerticle;
import com.hhzone.rlogex.index.IndexWriteVerticle;
import com.hhzone.rlogex.messages.ValueGetMessageBuilder;
import com.hhzone.rlogex.messages.ValueSetMessageBuilder;
import com.hhzone.rlogex.provider.HttpProviderVerticle;
import com.hhzone.rlogex.storage.StorageVerticle;
import com.hhzone.rlogex.storage.ValueType;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class ApplicationVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationVerticle.class);

    private static final String LOGS_RELOAD_PERIOD = "logs.reload.period";
    private static final String LOGS_RELOAD_PERIOD_DEFAULT = "PT1H";
    private static final String LOGS_BEFORE_DAYS = "logs.before.days";
    private static final int LOGS_BEFORE_DAYS_DEFAULT = -15;

    private Duration reloadPeriod;

    private long timerID;
    private MessageConsumer<JsonObject> messageConsumer;

    private boolean isRunning;
    private final Set<Date> runningFor = new HashSet<>();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        this.reloadPeriod = Duration.parse(config().getString(LOGS_RELOAD_PERIOD, LOGS_RELOAD_PERIOD_DEFAULT));
        deploy(new StorageVerticle(), startFuture::fail, r1 -> 
            deploy(new IndexWriteVerticle(), startFuture::fail, r2 ->
                deploy(new FileReaderVerticle(), startFuture::fail, r3 ->
                    deploy(new IndexSearchVerticle(), startFuture::fail, r4 ->
                        deploy(new HttpProviderVerticle(), startFuture::fail, r5 ->
                            postInit(startFuture))))));
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        vertx.cancelTimer(timerID);
        messageConsumer.unregister();
        stopFuture.complete();
    }

    private void deploy(final Verticle verticle, Handler<Throwable> onError, Handler<Void> onSuccess) {
        vertx.deployVerticle(verticle, new DeploymentOptions().setConfig(config()), handler -> {
          if (handler.succeeded()) {
              logger.info("Component is deployed {0}", verticle.getClass().getName());
              onSuccess.handle(null);
          }
          else
          {
              logger.error("Component deployment error {0}", verticle.getClass().getName(), handler.mapEmpty().cause());
              onError.handle(handler.mapEmpty().cause());
          }
        });
      }

    private void postInit(Future<Void> startFuture) {
        messageConsumer = vertx.eventBus().consumer("SCHEDULER", this::reload);
        timerID = vertx.setPeriodic(reloadPeriod.toMillis(), this::readFiles);
        logger.info("Application is started");
        startFuture.complete();
    }

    private void readFiles(long timer) {
        if (!isRunning) {
            isRunning = true;
            vertx.eventBus().send("STORAGE", new ValueGetMessageBuilder(ValueType.DATE, PersistentProperties.LAST_READED_PROPERTY_NAME, null).build(), (AsyncResult<Message<JsonObject>> r) -> {
                if (r.succeeded()) {
                    final Date current = trimDate(new Date());
                    final JsonObject response = r.result().body();
                    final Date lastReaded = nonNull(ValueType.DATE.getter().apply(response))
                            ? ValueType.DATE.<Date>getter().apply(response)
                            : plusDays(current, config().getInteger(LOGS_BEFORE_DAYS, LOGS_BEFORE_DAYS_DEFAULT));
                    final Date readForDate = plusDays(lastReaded, 1);
                    if (isPreviousDay(current, readForDate)) {
                        readFilesForDate(readForDate, m -> {
                            if (m.succeeded()) {
                                vertx.eventBus().send("STORAGE", new ValueSetMessageBuilder(ValueType.DATE, PersistentProperties.LAST_READED_PROPERTY_NAME, readForDate, null).build());
                            }
                            isRunning = false;
                        });
                    }
                }
            });
        }
    }

    private void reload(Message<JsonObject> message) {
        final Date reloadForDate = trimDate(Date.from(message.body().getInstant("date")));
        readFilesForDate(reloadForDate, m -> {
            if (m.succeeded()) {
                message.reply(null);
            }
            else {
                message.fail(2, m.cause().getMessage());
            }
        });
    }

    private void readFilesForDate(Date readForDate, Handler<AsyncResult<Void>> onComplete) {
        if (!runningFor.contains(readForDate)) {
            runningFor.add(readForDate);
            logger.info("Reading logs for {0}...", readForDate);
            new LogsReadingStrategy(readForDate, vertx, config()).start(m -> {
                if (m.succeeded()) {
                    logger.info("Reading logs for {0} is finised", readForDate);
                }
                else {
                    logger.error("Logs reading error {0}", readForDate, m.cause());                
                }
                runningFor.remove(readForDate);
                onComplete.handle(m);
            });
        }
        else {
            logger.warn("Logs reading already started for {0}", readForDate);
            onComplete.handle(Future.failedFuture("Logs reading already started for the date: " + readForDate));
        }
    }

    private static Date plusDays(Date date, int days) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);
        return cal.getTime();
    }

    private static boolean isPreviousDay(Date forDate, Date date) {
        final Calendar forCalendar = Calendar.getInstance();
        final Calendar calendar = Calendar.getInstance();
        forCalendar.setTime(forDate);
        calendar.setTime(date);
        return date.before(forDate) && (
                forCalendar.get(Calendar.YEAR) != calendar.get(Calendar.YEAR) || forCalendar.get(Calendar.DAY_OF_YEAR) != calendar.get(Calendar.DAY_OF_YEAR));
    }

    private static Date trimDate(Date date) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR, 0);
        return calendar.getTime();
    }
}
