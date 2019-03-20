package com.hhzone.rlogex.messages;

import io.vertx.core.json.JsonObject;

public abstract class AbstractMessageBuilder {
    protected JsonObject message = new JsonObject();

    public JsonObject build() {
        return message;
    }
}
