package com.hhzone.rlogex.provider;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import com.hhzone.rlogex.provider.HttpProviderVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LogsProviderVerticleTest {
    private static final int PORT = 8081;

    private Vertx vertx;

    @Before
    public void setUp(TestContext context) {
     final DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put("provider.http.port", PORT));
      vertx = Vertx.vertx();
      vertx.deployVerticle(HttpProviderVerticle.class.getName(), options, context.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext context) {
      vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testMyApplication(TestContext context) {
      final Async async = context.async();

      vertx.createHttpClient().getNow(PORT, "localhost", "/logs",
       response -> {
        final StringBuffer responseBody = new StringBuffer();
        response.handler(body -> {
            context.assertTrue(!body.toString().isEmpty());
            responseBody.append(body);
        });
        response.endHandler(v -> {
            context.assertTrue(responseBody.toString().contains("<title>Logs</title>"));
            async.complete();
        });
      });
    }
}
