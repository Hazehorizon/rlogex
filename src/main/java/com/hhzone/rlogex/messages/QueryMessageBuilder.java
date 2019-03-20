package com.hhzone.rlogex.messages;

public class QueryMessageBuilder extends AbstractMessageBuilder {
    public QueryMessageBuilder(String query, int page, int count, String startFrom, String finishTo) {
        message.put("query", query);
        message.put("page", page);
        message.put("count", count);
        message.put("start", startFrom);
        message.put("finish", finishTo);
    }
}
