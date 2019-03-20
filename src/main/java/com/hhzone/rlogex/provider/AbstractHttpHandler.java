package com.hhzone.rlogex.provider;

import io.vertx.core.eventbus.EventBus;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.templ.TemplateEngine;

public abstract class AbstractHttpHandler {
    private final TemplateEngine templateEngine;
    private final EventBus eventBus;

    public AbstractHttpHandler(TemplateEngine templateEngine, EventBus eventBus) {
        this.templateEngine = templateEngine;
        this.eventBus = eventBus;
    }

    protected void render(String templatePage, RoutingContext context) {
        templateEngine.render(context, "templates", templatePage, ar -> {
            if (ar.succeeded()) {
              context.response().putHeader("Content-Type", "text/html");
              context.response().end(ar.result());
            } else {
              context.fail(ar.cause());
            }
          });        
    }

    protected EventBus getEventBus() {
        return eventBus;
    }
}