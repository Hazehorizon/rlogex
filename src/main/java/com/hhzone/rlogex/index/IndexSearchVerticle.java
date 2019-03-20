package com.hhzone.rlogex.index;

import static java.util.Objects.nonNull;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;

import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class IndexSearchVerticle extends AbstractIndexVerticle {
    private static final String INDEX_SEARCH_POOL_SIZE = "index.search.pool.size";
    private static final int INDEX_SEARCH_POOL_SIZE_DEFAULT = 3;
    private static final String INDEX_SEARCH_POOL_EXECUTION_TIMEOUT = "index.search.pool.execution.timeout";
    private static final String INDEX_SEARCH_POOL_EXECUTION_TIMEOUT_DEFAULT = "PT1M";

    private MessageConsumer<?> messageConsumer;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        super.start();
        reader = DirectoryReader.open(dir);
        searcher = new IndexSearcher(reader);
        messageConsumer = vertx.eventBus().consumer("INDEX", this::search);
        logger.info("Index searcher is initialized");
        startFuture.complete();
    }

    @Override
    protected WorkerExecutor createWorker() {
        return vertx.createSharedWorkerExecutor("index-search-pool",
                config().getInteger(INDEX_SEARCH_POOL_SIZE, INDEX_SEARCH_POOL_SIZE_DEFAULT),
                Duration.parse(config().getString(INDEX_SEARCH_POOL_EXECUTION_TIMEOUT, INDEX_SEARCH_POOL_EXECUTION_TIMEOUT_DEFAULT)).toNanos());
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        messageConsumer.unregister();
        reader.close();
        logger.info("Index searcher is closed");
        super.stop();
        stopFuture.complete();
    }

    private void search(Message<JsonObject> message) {
        final String query = message.body().getString("query");
        final int page = message.body().getInteger("page");
        final int count = message.body().getInteger("count");
        final String startDate = message.body().getString("start");
        final String endDate = message.body().getString("finish");
        logger.debug("Processing {0} query, page: {1}, count: {2}, from {3} till {4} ...", query, page, count, startDate, endDate);
        workerPool.executeBlocking((Future<JsonObject> f) -> indexSearch(f, query, page, count, startDate, endDate), false, r -> {
            if (r.succeeded()) {
                logger.info("Query {0} processed", query);
                message.reply(r.result());
            } else {
                logger.error("Query {0} processing error", query, r.cause());
                message.fail(2, r.cause().getMessage());
            }
        });
    }

    private void indexSearch(Future<JsonObject> future, String query, int page, int count, String startDate, String endDate) { 
        try {
            final IndexSearcher search = getSearcher();
            final Query termQuery = new QueryParser("line", analyzer).parse(query);
            final TermRangeQuery rangeDateQuery = new TermRangeQuery("timestamp", new BytesRef(startDate), new BytesRef(endDate), true, true);
            final BooleanQuery combined = new BooleanQuery.Builder().add(termQuery, Occur.MUST).add(rangeDateQuery, Occur.MUST).build();

            ScoreDoc[] docs = search.search(combined, (page + 1)*count, new Sort(new SortField("timestamp", Type.STRING, true))).scoreDocs;
            final JsonArray docArray = new JsonArray();
            Arrays.stream(docs).skip(page*count).map(d -> {
                try {
                    return search.doc(d.doc);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).map(this::toJsonObject).forEach(docArray::add);
            future.complete(new JsonObject().put("lines", docArray));
        } catch (Exception e) {
            future.fail(e);
        }        
    }

    private JsonObject toJsonObject(Document document) {
        final JsonObject message = new JsonObject();
        document.forEach(f -> message.put(f.name(), f.stringValue()));
        return message;
    }

    private IndexSearcher getSearcher() throws IOException {
        final DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
        if (nonNull(newReader) && newReader != reader) {
          reader.close();
          reader = newReader;
          this.searcher = new IndexSearcher(reader);
          logger.debug("Index reader is re-opened");
        }
        return searcher;
    }
}
