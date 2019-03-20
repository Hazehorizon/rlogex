package com.hhzone.rlogex.provider;

import static java.util.Objects.nonNull;

import com.hhzone.rlogex.messages.QueryMessageBuilder;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.templ.TemplateEngine;

public class LogsHandler extends AbstractHttpHandler {
    private static final String PROVIDER_HTTP_LOGS_PAGE_SIZE = "provider.http.logs.page.size";
    private static final int PROVIDER_HTTP_LOGS_PAGE_SIZE_DEFAULT = 100;
    private final JsonObject config;

    public LogsHandler(TemplateEngine templateEngine, EventBus eventBus, JsonObject config) {
        super(templateEngine, eventBus);
        this.config = config;
    }

    public void logs(RoutingContext context) {
        final String query = orDefault(context.request().getParam("query"), "");
        final String page = orDefault(context.request().getParam("page"), 0);
        final String count = orDefault(context.request().getParam("count"), config.getInteger(PROVIDER_HTTP_LOGS_PAGE_SIZE, PROVIDER_HTTP_LOGS_PAGE_SIZE_DEFAULT));
        final LocalDateTime current = LocalDateTime.now().with(ChronoField.NANO_OF_DAY, 0);
        final String startDate = orDefault(context.request().getParam("startDateTime"), current.minusDays(1));
        final String endDate = orDefault(context.request().getParam("endDateTime"), current);
        final String url = context.request().absoluteURI();
        context.put("query", query).put("count", Integer.parseInt(count)).put("startDateTime", startDate).put("endDateTime", endDate);
        if (nonNull(query) && !query.isEmpty()) {
            final int currentPage = Integer.parseInt(page);
            context.put("page", currentPage).put("nextPageUrl", sameUrlWithPage(url, currentPage + 1));
            if (0 < currentPage) {
                context.put("previousPageUrl", sameUrlWithPage(url, currentPage - 1));
            }
            getEventBus().send("INDEX",
                    new QueryMessageBuilder(query, currentPage, Integer.parseInt(count), startDate, endDate).build(),
                    (AsyncResult<Message<JsonObject>> r) -> processResult(context, r));

        }
        else {
            render("/index.ftl", context);
        }
    }

    private void processResult(RoutingContext context, AsyncResult<Message<JsonObject>> result) {
        if (result.succeeded()) {
            render("/index.ftl", context.put("lines", result.result().body().getJsonArray("lines")));
        } else {
            context.fail(result.cause());
        }
    }

    private static String sameUrlWithPage(String url, int page) {
        return (url.replaceAll("page=\\d+&?", "") + "&page=" + page).replace("&&", "&");
    }

    private static String orDefault(String source, Object defaultValue) {
        return nonNull(source) && !source.isEmpty() ? source : defaultValue.toString();
    }
}
