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

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.query.evaluator.impl.QueryProcessor;
import com.querydsl.sql.SQLQuery;

@Component("queryEvaluator")
public class QueryEvaluator {
    @Autowired
    private QueryProcessor processor;

    public SQLQuery<?> evaluate(DataCollection dataCollection, Query query) {
        return processor.process(dataCollection, query);
    }

    public DataPage getResults(DataCollection dataCollection, Query query) {
        SQLQuery<?> sqlquery = evaluate(dataCollection, query);
        List<Map<String, Object>> data = new ArrayList<>();
        try (ResultSet results = sqlquery.getResults()) {
            ResultSetMetaData metadata = results.getMetaData();
            while (results.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= metadata.getColumnCount(); ++i) {
                    String columnName = metadata.getColumnName(i);
                    row.put(columnName, results.getObject(columnName));
                }
                data.add(row);
            }
            return new DataPage(data);

        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to retrieve data for object %s", query.getObjectType()), e);
        }
    }
}
