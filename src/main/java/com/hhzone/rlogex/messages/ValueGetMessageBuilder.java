package com.hhzone.rlogex.messages;

import static java.util.Objects.nonNull;

import com.hhzone.rlogex.storage.OperationType;
import com.hhzone.rlogex.storage.ValueType;
import java.util.Arrays;
import io.vertx.core.json.JsonArray;

public class ValueGetMessageBuilder extends AbstractMessageBuilder {
    public ValueGetMessageBuilder(ValueType type, String name, String... key) {
        message.put("type", type.name());
        message.put("name", name);
        message.put("operation", OperationType.GET.name());
        if (nonNull(key)) {
            final JsonArray keys = new JsonArray();
            Arrays.stream(key).forEach(keys::add);
            message.put("keys", keys);
        }
    }
}
