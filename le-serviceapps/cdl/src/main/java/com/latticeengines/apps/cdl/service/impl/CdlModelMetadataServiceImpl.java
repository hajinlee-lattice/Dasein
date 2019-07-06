package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CdlModelMetadataService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("cdlModelMetadataService")
public class CdlModelMetadataServiceImpl implements CdlModelMetadataService {

    private static final Logger log = LoggerFactory.getLogger(CdlModelMetadataServiceImpl.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public List<Table> cloneTrainingTargetTable(ModelSummary modelSummary) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        List<Table> tables = getTrainingTargetTableFromModelId(modelSummary);
        Table trainingClone = metadataProxy.cloneTable(customerSpace, tables.get(0).getName(), false);
        Table targetClone = metadataProxy.cloneTable(customerSpace, tables.get(1).getName(), false);
        metadataProxy.updateTable(customerSpace, trainingClone.getName() + "_TargetTable", targetClone);
        targetClone.setName(trainingClone.getName() + "_TargetTable");
        return Arrays.asList(trainingClone, targetClone);
    }

    @Override
    public List<Table> getTrainingTargetTableFromModelId(ModelSummary modelSummary) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        if (modelSummary == null) {
            throw new RuntimeException(String.format("No such model summary!"));
        }
        String modelId = modelSummary.getId();
        String trainingTableName = modelSummary.getTrainingTableName();
        Table trainingTable = getTableFromModelId(modelId, customerSpace, trainingTableName, "training");
        String targetTableName = modelSummary.getTargetTableName();
        Table targetTable = getTableFromModelId(modelId, customerSpace, targetTableName, "target");
        return Arrays.asList(trainingTable, targetTable);
    }

    private Table getTableFromModelId(String modelId, String customerSpace, String tableName, String tableType) {
        if (tableName == null) {
            throw new RuntimeException(String.format("Model %s does not have an %s table name", modelId, tableType));
        }

        Table table = metadataProxy.getTable(customerSpace, tableName);
        if (table == null) {
            throw new RuntimeException(
                    String.format("No %s table with name %s for model %s", tableType, tableName, modelId));
        }
        return table;
    }

}
