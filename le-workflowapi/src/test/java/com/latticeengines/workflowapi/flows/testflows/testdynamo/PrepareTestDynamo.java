package com.latticeengines.workflowapi.flows.testflows.testdynamo;

import static com.latticeengines.workflowapi.steps.core.ExportToDynamoDeploymentTestNG.TABLE_1;
import static com.latticeengines.workflowapi.steps.core.ExportToDynamoDeploymentTestNG.TABLE_2;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoTableConfig;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("prepareTestDynamo")
public class PrepareTestDynamo extends BaseWorkflowStep<BaseStepConfiguration> {

    @Override
    public void execute() {
        List<DynamoTableConfig> tables = Arrays.asList(
                new DynamoTableConfig(TABLE_1, "AccountId"),
                new DynamoTableConfig(TABLE_2, "AccountId", "ContactId" )
        );
        putObjectInContext(TABLES_GOING_TO_DYNAMO, tables);
    }

}
