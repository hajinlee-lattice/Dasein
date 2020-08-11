package com.latticeengines.dcp.workflow.steps;

import java.util.Set;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.RollupDataReportStepConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;

@Component("finishDataReport")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishDataReport  extends BaseSparkStep<RollupDataReportStepConfiguration> {


    @Override
    public void execute() {
        Set<String> names = getSetObjectFromContext(DUNS_COUNT_TABLE_NAMES, String.class);
        names.forEach(tableName -> {
            registerTable(tableName);
            Table table = metadataProxy.getTable(configuration.getCustomerSpace().toString(), tableName);
            exportToS3(table);
        });
    }


}
