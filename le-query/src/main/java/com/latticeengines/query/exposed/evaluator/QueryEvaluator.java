package com.latticeengines.query.exposed.evaluator;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.querydsl.sql.SQLQuery;

@Component("queryEvaluator")
public class QueryEvaluator {
    private static final Log log = LogFactory.getLog(QueryEvaluator.class);

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

            DataPage page = new DataPage(data);
            page.setMetadata(getMetadata(query.getLookups(), dataCollection));
            return page;
        } catch (SQLException e) {
            throw new LedpException(LedpCode.LEDP_37012, new String[] { query.getObjectType().toString() });
        }
    }

    private Map<String, ColumnMetadata> getMetadata(List<Lookup> lookups, DataCollection dataCollection) {
        Map<String, ColumnMetadata> metadataMap = new HashMap<>();
        for (Lookup lookup : lookups) {
            if (lookup instanceof ColumnLookup) {
                ColumnLookup columnLookup = (ColumnLookup) lookup;

                Table table = dataCollection.getTable(columnLookup.getObjectType());
                if (table == null) {
                    log.warn(String.format("Could not locate table with type %s to lookup metadata for lookup %s",
                            columnLookup.getObjectType(), columnLookup));
                    continue;
                }
                Attribute attribute = table.getAttribute(columnLookup.getColumnName());
                if (attribute == null) {
                    log.warn(String.format("Could not locate attribute with name %s to lookup metadata for lookup %s",
                            columnLookup.getColumnName(), columnLookup));
                    continue;
                }

                ColumnMetadata metadata = attribute.getColumnMetadata();
                metadataMap.put(columnLookup.getColumnName().toLowerCase(), metadata);
            }
        }
        return metadataMap;
    }
}
