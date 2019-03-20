package com.hhzone.rlogex.storage;

import static java.util.Objects.isNull;

import java.util.Date;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;

import io.vertx.core.json.JsonObject;

public enum ValueType {
    DATE(Serializer.DATE,
        (o, v) -> o.put("value", isNull(v) ? null : v.toInstant()),
        o -> Optional.ofNullable(o.getInstant("value")).map(Date::from).orElse(null) ),
    LONG(Serializer.LONG, (o, v) -> o.put("value", v), o -> o.getLong("value"));

    private final GroupSerializer<?> serializer;
    private final BiConsumer<JsonObject, ? extends Object> setter;
    private final Function<JsonObject, ? extends Object> getter;

    private <T> ValueType(GroupSerializer<T> serializer, BiConsumer<JsonObject, T> setter, Function<JsonObject, T> getter) {
        this.serializer = serializer;
        this.setter = setter;
        this.getter = getter;
    }

    public <T> GroupSerializer<T> serializer() {
        
        return (GroupSerializer<T>)serializer;
    }

    public <T> BiConsumer<JsonObject, T> setter() {
        return (BiConsumer<JsonObject, T>)setter;
    }

    public <T> Function<JsonObject, T> getter() {
        return (Function<JsonObject, T>)getter;
    }

    public static Optional<ValueType> optionalValueOf(String name) {
        try {
            return isNull(name) ? Optional.empty() : Optional.of(valueOf(name));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
