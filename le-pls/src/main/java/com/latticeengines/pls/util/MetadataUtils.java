package com.latticeengines.pls.util;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public final class MetadataUtils {

    public static final Table getEventTableFromModelId(String modelId, //
            ModelSummaryEntityMgr modelSummaryEntityMgr, //
            MetadataProxy metadataProxy) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        ModelSummary modelSummary = modelSummaryEntityMgr.findValidByModelId(modelId);
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
