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
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.prestodb.exposed.service.AthenaService;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.latticeengines.query.factory.AthenaQueryProvider;
import com.latticeengines.query.factory.PrestoQueryProvider;
import com.querydsl.sql.SQLQuery;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component("queryEvaluator")
public class QueryEvaluator {

    public static final String SCORE = "Score";
    private static final int MAX_CARDINALITY = 10_000_000;

    private static Scheduler scheduler = null;

    @Inject
    private QueryProcessor processor;

    @Inject
    private AthenaService athenaService;

    public SQLQuery<?> evaluate(AttributeRepository repository, Query query, String sqlUser) {
        if (processor == null) {
            throw new RuntimeException("processor is null.");
        }
        return processor.process(repository, query, sqlUser);
    }

    Flux<Map<String, Object>> pipe(SQLQuery<?> sqlquery, Query query, String sqlUser) {
        final Map<String, String> attrNames = new HashMap<>();
        query.getLookups().forEach(l -> {
            if (l instanceof AttributeLookup) {
                String attrName = ((AttributeLookup) l).getAttribute();
                attrNames.put(attrName.toLowerCase(), attrName);
            } else if (l instanceof AggregateLookup) {
                Lookup lookup = ((AggregateLookup) l).getLookup();
                String alias = ((AggregateLookup) l).getAlias();
                if (alias != null && (lookup instanceof AttributeLookup)) {
                    String attrName = ((AttributeLookup) lookup).getAttribute();
                    attrNames.put(attrName.toLowerCase(), alias);
                } else if (alias == null && (lookup instanceof AttributeLookup)) {
                    String attrName = ((AttributeLookup) lookup).getAttribute();
                    attrNames.put(attrName.toLowerCase(), attrName);
                }
            }
        });
        attrNames.put(SCORE.toLowerCase(), SCORE);

        long offset = 0L, limit = 0L;
        if (query.getPageFilter() != null && query.containEntityForExists()) {
            offset = query.getPageFilter().getRowOffset();
            limit = query.getPageFilter().getNumRows();
        }

        Flux<Map<String, Object>> flux;
        if (AthenaQueryProvider.ATHENA_USER.equals(sqlUser)) {
            flux = getAthenaFlux(sqlquery, attrNames, offset, limit);
        } else {
            sqlquery.setUseLiterals(PrestoQueryProvider.PRESTO_USER.equals(sqlUser));
            flux = getSQLFlux(sqlquery, attrNames, offset, limit);
        }
        return flux.subscribeOn(getScheduler());
    }

    private Flux<Map<String, Object>> getSQLFlux(SQLQuery<?> sqlquery, Map<String, String> attrNames, //
                                                 final long offset, final long limit) {
        AtomicLong iter = new AtomicLong(0);
        return Flux.generate(() -> {
            ResultSet results;
            results = sqlquery.getResults();
            ResultSetMetaData metadata = results.getMetaData();
            int fetchSize = getFetchSize(metadata.getColumnCount());
            results.setFetchSize(fetchSize);
            return results;
        }, (results, sink) -> {
            try {
                if (limit > 0 && iter.get() >= offset + limit) {
                    results.close();
                    sink.complete();
                } else {
                    while (iter.get() < offset) {
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
    }

    private Flux<Map<String, Object>> getAthenaFlux(SQLQuery<?> sqlquery, Map<String, String> attrNames, //
                                                 final long offset, final long limit) {
        sqlquery.setUseLiterals(true);
        String sql = sqlquery.getSQL().getSQL();
        Flux<Map<String, Object>> flux = athenaService.queryFlux(sql);
        if (offset > 0) {
            flux = flux.skip(offset);
        }
        if (limit > 0) {
            flux = flux.take(limit);
        }
        return flux.map(row -> {
            Map<String, Object> row2 = new HashMap<>();
            row.forEach((k, v) -> {
                String attr = attrNames.getOrDefault(k, k);
                row2.put(attr, v);
            });
            return row2;
        });
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
