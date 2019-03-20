package com.hhzone.rlogex.filereader;

import java.util.function.Function;

import io.vertx.core.json.JsonObject;

public enum LogType {
    ACCESS("access", JsonObjectBuilders::buildAccessLineMessage),
    SERVER("server", JsonObjectBuilders::buildLineMessage),
    DEVICE("device", JsonObjectBuilders::buildDeviceLineMessage);

    private final String name;
    private final Function<LineContext, JsonObject> builder;

    private LogType(String name, Function<LineContext, JsonObject> builder) {
        this.name = name;
        this.builder = builder;
    }

    public String getName() {
        return name;
    }

    public Function<LineContext, JsonObject> getBuilder() {
        return builder;
    }
}
