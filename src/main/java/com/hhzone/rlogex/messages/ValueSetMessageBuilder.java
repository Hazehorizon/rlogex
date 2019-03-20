package com.hhzone.rlogex.messages;

import static java.util.Objects.nonNull;

import com.hhzone.rlogex.storage.OperationType;
import com.hhzone.rlogex.storage.ValueType;
import java.util.Arrays;
import io.vertx.core.json.JsonArray;

public class ValueSetMessageBuilder extends AbstractMessageBuilder {
    public ValueSetMessageBuilder(ValueType type, String name, Object value, String... key) {
        message.put("type", type.name());
        message.put("name", name);
        type.setter().accept(message, value);
        message.put("operation", OperationType.SET.name());
        if (nonNull(key)) {
            final JsonArray keys = new JsonArray();
            Arrays.stream(key).forEach(keys::add);
            message.put("keys", keys);
        }
    }
}
