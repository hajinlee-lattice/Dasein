package com.latticeengines.pls.util;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public final class MetadataUtils {

    public static Table getEventTableFromModelId(String modelId, //
           ModelSummaryProxy modelSummaryProxy, //
           MetadataProxy metadataProxy) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        ModelSummary modelSummary = modelSummaryProxy.findValidByModelId(customerSpace, modelId);
        if (modelSummary == null) {
            throw new RuntimeException(String.format("No such model summary with id %s", modelId));
        }
        String tableName = modelSummary.getEventTableName();
        if (tableName == null) {
            throw new RuntimeException(String.format("Model %s does not have an event tableName", modelId));
        }

        Table table = metadataProxy.getTable(customerSpace, tableName);
        if (table == null) {
            throw new RuntimeException(String.format("No such table with name %s for model %s", tableName, modelId));
        }
        return table;
    }
}
