package com.latticeengines.workflowapi.flows.testflows.redshift;

import static com.latticeengines.workflowapi.steps.core.BaseExportDeploymentTestNG.TABLE_1;
import static com.latticeengines.workflowapi.steps.core.BaseExportDeploymentTestNG.TABLE_2;
import static com.latticeengines.workflowapi.steps.core.BaseExportDeploymentTestNG.TABLE_3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("prepareTestRedshift")
public class PrepareTestRedshift extends BaseWorkflowStep<PrepareTestRedshiftConfiguration> {

    @Override
    public void execute() {
        List<RedshiftExportConfig> tables;
        if (configuration.isUpdateMode()) {
            tables = Collections.singletonList( //
                    table3Config());
        } else {
            tables = Arrays.asList( //
                    table1Config(), //
                    table2Config());
        }
        putObjectInContext(TABLES_GOING_TO_REDSHIFT, tables);
    }

    private RedshiftExportConfig table1Config() {
        RedshiftExportConfig config = tableConfig(TABLE_1);
        config.setDistKey("AccountId");
        config.setSingleSortKey("AccountId");
        return config;
    }

    private RedshiftExportConfig table2Config() {
        RedshiftExportConfig config = tableConfig(TABLE_2);
        config.setDistKey("ContactId");
        config.setSortKeys(Arrays.asList("AccountId", "ContactId"));
        return config;
    }

    private RedshiftExportConfig table3Config() {
        RedshiftExportConfig config = tableConfig(TABLE_1);
        config.setInputPath(inputPathForTable(TABLE_3));
        config.setDistKey("AccountId");
        config.setSingleSortKey("AccountId");
        config.setUpdateMode(true);
        return config;
    }

    private RedshiftExportConfig tableConfig(String tableName) {
        RedshiftExportConfig config = new RedshiftExportConfig();
        String tenantId = CustomerSpace.shortenCustomerSpace(configuration.getCustomerSpace());
        config.setTableName(tenantId + "_" + tableName);
        config.setInputPath(inputPathForTable(tableName));
        return config;
    }

    private String inputPathForTable(String tableName) {
        String podId = CamilleEnvironment.getPodId();
        CustomerSpace customerSpace = CustomerSpace.parse(configuration.getCustomerSpace());
        return PathBuilder.buildDataTablePath(podId, customerSpace).append(tableName).toString();
    }

}
