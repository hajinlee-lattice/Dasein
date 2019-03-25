package com.latticeengines.query.exposed.evaluator;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.querydsl.sql.SQLQuery;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component("queryEvaluator")
public class QueryEvaluator {

    public static final String SCORE = "Score";
    private static final int MAX_CARDINALITY = 1_000_000;

    private static Scheduler scheduler = null;

    @Inject
    private QueryProcessor processor;

    public SQLQuery<?> evaluate(AttributeRepository repository, Query query, String sqlUser) {
        if (processor == null) {
            throw new RuntimeException("processor is null.");
        }
        return processor.process(repository, query, sqlUser);
    }

    Flux<Map<String, Object>> pipe(SQLQuery<?> sqlquery, Query query) {
        final Map<String, String> attrNames = new HashMap<>();
        query.getLookups().forEach(l -> {
            if (l instanceof AttributeLookup) {
                String attrName = ((AttributeLookup) l).getAttribute();
                attrNames.put(attrName.toLowerCase(), attrName);
            }
        });
        attrNames.put(SCORE.toLowerCase(), SCORE);

        long offset = 0L, limit = 0L;
        if (query.getPageFilter() != null && query.containEntityForExists()) {
            offset = query.getPageFilter().getRowOffset();
            limit = query.getPageFilter().getNumRows();
        }

        final long finalOffset = offset;
        final long finalLimit = limit;
        AtomicLong iter = new AtomicLong(0);
        Flux<Map<String, Object>> flux = Flux.generate(() -> {
            ResultSet results = sqlquery.getResults();
            ResultSetMetaData metadata = results.getMetaData();
            int fetchSize = getFetchSize(metadata.getColumnCount());
            results.setFetchSize(fetchSize);
            return results;
        }, (results, sink) -> {
            try {
                if (finalLimit > 0 && iter.get() >= finalOffset + finalLimit) {
                    results.close();
                    sink.complete();
                } else {
                    while (iter.get() < finalOffset) {
                        if (!results.next()) {
                            results.close();
                            sink.complete();
                        }
                        iter.incrementAndGet();
                    }
                    if (results.next()) {
                        Map<String, Object> row = readRow(attrNames, results);
                        sink.next(row);
                        iter.incrementAndGet();
                    } else {
                        results.close();
                        sink.complete();
                    }
                }
            } catch (SQLException e) {
                sink.error(e);
            }
            return results;
        });
        return flux.subscribeOn(getScheduler());
    }

    private static Map<String, Object> readRow(Map<String, String> attrNames, ResultSet results) throws SQLException {
        Map<String, Object> row = new HashMap<>();
        for (int i = 1; i <= results.getMetaData().getColumnCount(); ++i) {
            String columnName = results.getMetaData().getColumnName(i);
            Object value = results.getObject(columnName);
            String attrName = attrNames.get(columnName);
            columnName = (attrName == null) ? columnName : attrName;
            row.put(columnName, value);
        }
        return row;
    }

    private static int getFetchSize(int columnCount) {
        return Math.max(10, MAX_CARDINALITY / columnCount);
    }

    private Scheduler getScheduler() {
        if (scheduler == null) {
            createScheduler();
        }
        return scheduler;
    }

    private synchronized void createScheduler() {
        scheduler = Schedulers.newParallel("fetch-redshift", 16);
    }

}
