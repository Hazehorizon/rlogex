package com.hhzone.rlogex.index;

import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public abstract class AbstractIndexVerticle extends AbstractVerticle {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractIndexVerticle.class);
    protected Directory dir;
    protected Analyzer analyzer;
    protected WorkerExecutor workerPool;

    @Override
    public void start() throws Exception {
        final String indexDir = config().getString("index.dir", "index");
        analyzer = CustomAnalyzer.builder()
                .withTokenizer(StandardTokenizerFactory.class)
                .addTokenFilter(StandardFilterFactory.class)
                .addTokenFilter(StopFilterFactory.class/*, "ignoreCase", "false", "words", "stopwords.txt", "format", "wordset"*/)
                .build();
//        analyzer = new StandardAnalyzer();
//  https://stackoverflow.com/questions/34422397/why-my-version-of-case-insensitive-lucene-keyword-analyzer-is-not-working
        dir = FSDirectory.open(Paths.get(indexDir));
        workerPool = createWorker();
        logger.info("Index opened from {0}", indexDir);
    }

    @Override
    public void stop() throws Exception {
        workerPool.close();
        dir.close();
        analyzer.close();
        logger.info("Index closed");
    }

    protected abstract WorkerExecutor createWorker();
}
