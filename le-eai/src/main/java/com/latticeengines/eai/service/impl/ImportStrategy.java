package com.latticeengines.eai.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.ProducerTemplate;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Table;

public abstract class ImportStrategy {

    private static Map<String, ImportStrategy> strategies = new HashMap<>();
    
    protected ImportStrategy(String key) {
        strategies.put(key, this);
    }
    
    public static ImportStrategy getImportStrategy(SourceType sourceType, String tableName) {
        return strategies.get(sourceType.getName() + "." + tableName);
    }

    public static ImportStrategy getImportStrategy(SourceType sourceType, Table table) {
        return getImportStrategy(sourceType, table.getName());
    }
    
    public abstract void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx);
    
    public abstract Table importMetadata(ProducerTemplate template, Table table, String filter, ImportContext ctx);
    
    public abstract ImportContext resolveFilterExpression(String expression, ImportContext ctx);
    
    protected abstract AvroTypeConverter getAvroTypeConverter();
}
