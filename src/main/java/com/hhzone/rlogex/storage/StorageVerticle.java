package com.hhzone.rlogex.storage;

import static java.util.Objects.isNull;

import java.util.Arrays;
import java.util.NavigableMap;
import java.util.Optional;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class StorageVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(StorageVerticle.class);

    private DB db;
    private MessageConsumer<?> messageConsumer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        final String dbFileName = config().getString("storage.fileName", "rlogex.db");
        db = DBMaker.fileDB(dbFileName).closeOnJvmShutdown().make();
        logger.info("Local DB is opened: {0}", dbFileName);
        messageConsumer = vertx.eventBus().consumer("STORAGE", this::process);
        startFuture.complete();
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        messageConsumer.unregister();
//        db.close();
        logger.info("Local DB is closed");
        stopFuture.complete();
    }

    private void process(Message<JsonObject> message) {
        final String name = message.body().getString("name");
        final Object[] key = key(message.body().getJsonArray("keys"));
        final Optional<ValueType> type = ValueType.optionalValueOf(message.body().getString("type"));
        switch (OperationType.valueOf(message.body().getString("operation"))) {
            case GET:
                final JsonObject answer = isNull(key) ? getSingleValue(name, type.get()) : getMapValues(name, key, type.get());
                db.commit();
                logger.debug("{0}.{1} returned {2}", name, Arrays.toString(key), answer);
                message.reply(answer);
                break;
            case SET:
                if (isNull(key)) {
                    db.atomicVar(name, type.get().serializer()).createOrOpen().set(type.get().getter().apply(message.body()));
                }
                else {
                    db.treeMap(name, new SerializerArrayTuple(Serializer.STRING, Serializer.STRING, Serializer.STRING), type.get().serializer())
                        .createOrOpen()
                        .put(key, type.get().getter().apply(message.body()));                    
                }
                db.commit();
                logger.debug("{0}.{1} set to {2}", name, Arrays.toString(key), type.get().getter().apply(message.body()));
                message.reply(null);
                break;
            case REMOVE:
                if (isNull(key)) {
                    db.atomicVar(name).createOrOpen().set(null);
                }
                else {
                    db.treeMap(name).createOrOpen().remove(key);                   
                }
                db.commit();
                logger.debug("{0}.{1} removed", name, Arrays.toString(key));
                message.reply(null);                
                break;
        }
    }

    private JsonObject getSingleValue(String name, ValueType type) {
        final Object value = db.atomicVar(name, type.serializer()).createOrOpen().get();
        final JsonObject answer = new JsonObject();
        answer.put("name", name);
        type.setter().accept(answer, value);
        return answer;
    }

    private JsonObject getMapValues(String name, Object[] key, ValueType type) {
        final JsonObject answer = new JsonObject();
        final NavigableMap<Object[], Object> result = db.treeMap(name, new SerializerArrayTuple(Serializer.STRING, Serializer.STRING, Serializer.STRING), type.serializer())
                .createOrOpen()
                .prefixSubMap(key);
        final JsonArray elements = new JsonArray();
        result.entrySet().forEach(e -> {
            final JsonObject element = new JsonObject();
            element.put("keys", keyJson(e.getKey()));
            type.setter().accept(element, e.getValue());
            elements.add(element);
        });
        answer.put("name", name);
        answer.put("values", elements);
        return answer;
    }

    private static final Object[] key(JsonArray array) {
        return isNull(array) ? null : array.stream().toArray();
    }

    private static JsonArray keyJson(Object[] keys) {
        final JsonArray array = new JsonArray();
        Arrays.stream(keys).forEach(array::add);
        return array;
    }
}
