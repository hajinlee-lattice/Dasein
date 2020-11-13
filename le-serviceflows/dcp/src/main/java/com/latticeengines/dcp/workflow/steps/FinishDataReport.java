package com.latticeengines.dcp.workflow.steps;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.RollupDataReportStepConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;

@Component("finishDataReport")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishDataReport  extends BaseSparkStep<RollupDataReportStepConfiguration> {


    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        String customerSpaceStr = customerSpace.toString();
        Set<String> names = getSetObjectFromContext(DUNS_COUNT_TABLE_NAMES, String.class);
        ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("rollup", 16);
        names.forEach(tableName -> {
            registerTable(tableName);
            Table table = metadataProxy.getTable(customerSpaceStr, tableName);
            exportToS3(table, false);
        });
    }

    @Override
    protected ExecutorService getS3ExporterPool() {
        return ThreadPoolUtils.getFixedSizeThreadPool("rollup", 16);
    }
}
