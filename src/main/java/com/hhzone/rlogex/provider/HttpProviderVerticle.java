package com.hhzone.rlogex.provider;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;

public class HttpProviderVerticle extends AbstractVerticle {
    private static final String HTTP_PORT = "provider.http.port";
    private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        final HttpServerOptions serverOptions = new HttpServerOptions();
        serverOptions.setCompressionSupported(true);

        final Router router = Router.router(vertx);

        final LogsHandler logsHttp = new LogsHandler(templateEngine, vertx.eventBus(), config());
        router.get("/logs").handler(logsHttp::logs);

        final AdminHandler adminHttp = new AdminHandler(templateEngine, vertx.eventBus());
        router.get("/admin").handler(adminHttp::admin);
        router.get("/admin/remove").handler(adminHttp::removeFileDate);
        router.get("/admin/reload").handler(adminHttp::reloadDate);
        router.get("/admin/setDate").handler(adminHttp::setDate);
        router.get("/admin/resetDate").handler(adminHttp::resetDate);

        vertx.createHttpServer(serverOptions).requestHandler(router::accept)
        .listen(config().getInteger(HTTP_PORT, 8080), result -> {
          if (result.succeeded()) {
              startFuture.complete();
          } else {
              startFuture.fail(result.cause());
          }
        });
    }
}
