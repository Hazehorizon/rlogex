package com.hhzone.rlogex.index;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.lucene70.Lucene70Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class IndexWriteVerticle extends AbstractIndexVerticle {
    private static final String INDEX_WRITE_POOL_SIZE = "index.write.pool.size";
    private static final int INDEX_WRITE_POOL_SIZE_DEFAULT = 5;
    private static final String INDEX_WRITE_POOL_EXECUTION_TIMEOUT = "index.write.pool.execution.timeout";
    private static final String INDEX_WRITE_POOL_EXECUTION_TIMEOUT_DEFAULT = "PT1M";
    private static final String INDEX_WRITE_COMMIT_TIMEOUT = "index.write.commit.timeout";
    private static final String INDEX_WRITE_COMMIT_TIMEOUT_DEFAULT = "PT2M";

    private IndexWriter writer;
    private MessageConsumer<?> messageConsumer;
    private boolean indexChanged;
    private long timerID;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        super.start();
        final IndexWriterConfig config = new IndexWriterConfig(analyzer).setCodec(new Lucene70Codec(Mode.BEST_COMPRESSION));
        writer = new IndexWriter(dir, config);
        writer.commit();
        messageConsumer = vertx.eventBus().consumer("INDEX_WRITE", this::write);
        timerID = vertx.setPeriodic(Duration.parse(config().getString(INDEX_WRITE_COMMIT_TIMEOUT, INDEX_WRITE_COMMIT_TIMEOUT_DEFAULT)).toMillis(), this::commit);
        logger.info("Index writer is initialized");
        startFuture.complete();
    }

    @Override
    protected WorkerExecutor createWorker() {
        return vertx.createSharedWorkerExecutor("index-write-pool",
                config().getInteger(INDEX_WRITE_POOL_SIZE, INDEX_WRITE_POOL_SIZE_DEFAULT),
                Duration.parse(config().getString(INDEX_WRITE_POOL_EXECUTION_TIMEOUT, INDEX_WRITE_POOL_EXECUTION_TIMEOUT_DEFAULT)).toNanos());
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        messageConsumer.unregister();
        vertx.cancelTimer(timerID);
        writer.close();
        logger.info("Index writer is closed");
        super.stop();
        stopFuture.complete();
    }

    private void write(Message<JsonObject> message) {
        workerPool.executeBlocking(f -> {
            try {
                final JsonArray lines = message.body().getJsonArray("lines");
                final Term[] idTerms = lines.stream().map(o -> (JsonObject)o).map(IndexWriteVerticle::createIdTerm).toArray(Term[]::new);
                final List<Document> documents = lines.stream().map(o -> (JsonObject)o).map(IndexWriteVerticle::createDocument).collect(toList());
                writer.deleteDocuments(idTerms);
                writer.addDocuments(documents);
                f.complete(lines.size());
            } catch (IOException ex) {
                f.fail(ex);
            }            
        }, false, r -> {
            indexChanged = true;
            if (r.succeeded()) {
                logger.trace("Write {0} documents to index", r.result());
                message.reply(null);
            }
            else {
                message.fail(2, r.cause().getMessage());
            }
        });
    }

    private void commit(long timer) {
        if (indexChanged) {
            try {
                long indexes = writer.commit();
                indexChanged = false;
                logger.info("Index commit {0}", indexes);
            } catch (IOException e) {
                logger.error("Index commit error", e);
                throw new IndexCommitException(e);
            }
        }
    }

    private static Document createDocument(JsonObject json) {
        final Document doc = new Document();
        json.forEach(e -> {
                switch (e.getKey()) {
                    case "line" :
                        doc.add(new TextField(e.getKey(), e.getValue().toString(), Field.Store.YES));
                        break;
                    case "timestamp" :
                        doc.add(new StringField(e.getKey(), e.getValue().toString(), Field.Store.YES));
                        doc.add(new SortedDocValuesField(e.getKey(), new BytesRef(e.getValue().toString())));
                        break;
                    default:
                        doc.add(new StringField(e.getKey(), e.getValue().toString(), Field.Store.YES));
                        break;
                }
        });
        return doc;
    }

    private static Term createIdTerm(JsonObject json) {
        return new Term("id", json.getInteger("id").toString());
    }

    public static class IndexCommitException extends RuntimeException {
        public IndexCommitException(Throwable cause) {
            super(cause);
        }
    }
}
