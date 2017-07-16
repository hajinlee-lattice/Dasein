package com.latticeengines.query.exposed.evaluator;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.querydsl.sql.SQLQuery;

@Component("queryEvaluator")
public class QueryEvaluator {

    @Autowired
    private QueryProcessor processor;

    public SQLQuery<?> evaluate(AttributeRepository repository, Query query) {
        if (processor == null) {
            throw new RuntimeException("processor is null.");
        }
        return processor.process(repository, query);
    }

    public DataPage run(AttributeRepository repository, Query query) {
        SQLQuery<?> sqlquery = evaluate(repository, query);
        return run(sqlquery, repository, query);
    }

    public DataPage run(SQLQuery<?> sqlquery, AttributeRepository repository, Query query) {
        List<Map<String, Object>> data = new ArrayList<>();
        final Map<String, String> attrNames = new HashMap<>();
        query.getLookups().forEach(l -> {
            if (l instanceof AttributeLookup) {
                String attrName = ((AttributeLookup) l).getAttribute();
                attrNames.put(attrName.toLowerCase(), attrName);
            }
        });

        try (ResultSet results = sqlquery.getResults()) {
            ResultSetMetaData metadata = results.getMetaData();
            while (results.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= metadata.getColumnCount(); ++i) {
                    String columnName = metadata.getColumnName(i);
                    Object value = results.getObject(columnName);
                    String attrName = attrNames.get(columnName);
                    columnName = (attrName == null) ? columnName : attrName;
                    row.put(columnName, value);
                }
                data.add(row);
            }
            return new DataPage(data);
        } catch (SQLException e) {
            throw new LedpException(LedpCode.LEDP_37012, new String[] { repository.getIdentifier() });
        }
    }
}
