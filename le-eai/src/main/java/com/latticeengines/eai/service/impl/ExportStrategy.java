package com.latticeengines.eai.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.ProducerTemplate;

import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.metadata.Table;

public abstract class ExportStrategy {

    private static Map<ExportFormat, ExportStrategy> strategies = new HashMap<>();

    protected ExportStrategy(ExportFormat key) {
        strategies.put(key, this);
    }

    public static ExportStrategy getExportStrategy(ExportFormat exportFormat) {
        return strategies.get(exportFormat);
    }

    public abstract void exportData(ProducerTemplate template, Table table, String filter, ExportContext ctx);
}
