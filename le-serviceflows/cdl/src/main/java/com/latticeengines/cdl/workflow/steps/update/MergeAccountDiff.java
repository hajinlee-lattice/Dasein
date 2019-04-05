package com.latticeengines.cdl.workflow.steps.update;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;

@Component
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeAccountDiff extends BaseMergeTableRoleDiff<ProcessAccountStepConfiguration> {

    @Override
    protected TableRoleInCollection getTableRole() {
        return BusinessEntity.Account.getServingStore();
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        super.postJobExecution(result);
        exportToDynamo();
    }

    private void exportToDynamo() {
        String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), mergedTableName);
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(mergedTableName);
        config.setInputPath(inputPath + "/*.avro");
        config.setPartitionKey(InterfaceName.AccountId.name());
        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }

}
