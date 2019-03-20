package com.hhzone.rlogex.messages;

import static java.util.Objects.nonNull;

import com.hhzone.rlogex.storage.OperationType;
import java.util.Arrays;
import io.vertx.core.json.JsonArray;

public class ValueRemoveMessageBuilder extends AbstractMessageBuilder {
    public ValueRemoveMessageBuilder(String name, String... key) {
        message.put("name", name);
        message.put("operation", OperationType.REMOVE.name());
        if (nonNull(key)) {
            final JsonArray keys = new JsonArray();
            Arrays.stream(key).forEach(keys::add);
            message.put("keys", keys);
        }
    }
}
