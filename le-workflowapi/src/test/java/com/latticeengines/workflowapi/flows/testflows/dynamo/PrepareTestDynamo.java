package com.latticeengines.workflowapi.flows.testflows.dynamo;

import static com.latticeengines.workflowapi.steps.core.BaseExportDeploymentTestNG.TABLE_1;
import static com.latticeengines.workflowapi.steps.core.BaseExportDeploymentTestNG.TABLE_2;
import static com.latticeengines.workflowapi.steps.core.BaseExportDeploymentTestNG.TABLE_3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.workflowapi.flows.testflows.framework.TestBasePreprocessingStep;

@Component("prepareTestDynamo")
public class PrepareTestDynamo extends TestBasePreprocessingStep<PrepareTestDynamoConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PrepareTestDynamo.class);

    @Override
    public void execute() {
        List<DynamoExportConfig> tables;
        if (configuration.isUpdateMode()) {
            tables = Collections.singletonList(
                    tableConfig(TABLE_3, TABLE_1, "AccountId")
            );
        } else {
            tables = Arrays.asList(
                    tableConfig(TABLE_1, TABLE_1, "AccountId"),
                    tableConfig(TABLE_2, TABLE_2, "AccountId", "ContactId")
            );
        }
        putObjectInContext(TABLES_GOING_TO_DYNAMO, tables);
    }

    private DynamoExportConfig tableConfig(String srcTbl, String tgtTbl, String partitionKey) {
        return tableConfig(srcTbl, tgtTbl, partitionKey, null);
    }

    private DynamoExportConfig tableConfig(String srcTbl, String tgtTbl, String partitionKey, String sortKey) {
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(tgtTbl);
        config.setInputPath(inputPathForTable(srcTbl));
        config.setPartitionKey(partitionKey);
        if (StringUtils.isNotBlank(sortKey)) {
            config.setSortKey(sortKey);
        }
        return config;
    }

    private String inputPathForTable(String tableName) {
        String podId = CamilleEnvironment.getPodId();
        CustomerSpace customerSpace = CustomerSpace.parse(configuration.getCustomerSpace());
        return PathBuilder.buildDataTablePath(podId, customerSpace).append(tableName).toString();
    }

}
