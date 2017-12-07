package com.latticeengines.cdl.workflow.steps.update;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("processProductDiff")
public class ProcessProductDiff extends BaseWorkflowStep<ProcessProductStepConfiguration> {

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        BusinessEntity entity = BusinessEntity.Product;
        String customerSpace = configuration.getCustomerSpace().toString();

        Map<BusinessEntity, String> diffTableNames = getMapObjectFromContext(ENTITY_DIFF_TABLES,
                BusinessEntity.class, String.class);
        String diffTableName = diffTableNames.get(entity);

        Table redshiftTable = metadataProxy.getTable(customerSpace, diffTableName);
        if (redshiftTable == null) {
            throw new RuntimeException("Diff table has not been created.");
        }
        Map<BusinessEntity, String> entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                BusinessEntity.class, String.class);
        if (entityTableMap == null) {
            entityTableMap = new HashMap<>();
        }
        entityTableMap.put(entity, diffTableName);
        putObjectInContext(TABLE_GOING_TO_REDSHIFT, entityTableMap);

        Map<BusinessEntity, Boolean> appendTableMap = getMapObjectFromContext(APPEND_TO_REDSHIFT_TABLE,
                BusinessEntity.class, Boolean.class);
        if (appendTableMap == null) {
            appendTableMap = new HashMap<>();
        }
        appendTableMap.put(entity, true);
        putObjectInContext(APPEND_TO_REDSHIFT_TABLE, appendTableMap);
    }

}
