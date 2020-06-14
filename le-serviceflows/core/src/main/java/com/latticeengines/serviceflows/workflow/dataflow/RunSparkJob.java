package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;

public abstract class RunSparkJob<S extends BaseStepConfiguration, C extends SparkJobConfig> //
        extends BaseSparkStep<S> { //

    private static final Logger log = LoggerFactory.getLogger(RunSparkJob.class);

    protected abstract Class<? extends AbstractSparkJob<C>> getJobClz();

    /**
     * Set job config except jobName and workspace.
     */
    protected abstract C configureJob(S stepConfiguration);

    protected abstract void postJobExecution(SparkJobResult result);

    @Override
    public void execute() {
        log.info("Executing spark job " + getJobClz().getSimpleName());
        customerSpace = parseCustomerSpace(configuration);
        C jobConfig = configureJob(configuration);
        if (jobConfig != null) {
            SparkJobResult result = runSparkJob(getJobClz(), jobConfig);
            postJobExecution(result);
        } else {
            log.info("Spark job config is null, skip submitting spark job.");
        }
        clearAllWorkspacesAsync();
    }

    protected void overlayTableSchema(Table resultTable, Map<String, Attribute> attributeMap) {
        List<Attribute> attrs = resultTable.getAttributes();
        List<Attribute> newAttrs = attrs.stream().map(attr -> {
            String attrName = attr.getName();
            return attributeMap.getOrDefault(attrName, attr);
        }).collect(Collectors.toList());
        resultTable.setAttributes(newAttrs);
    }

    protected boolean allTablesExist(Map<String, String> tableNames) {
        String customer = customerSpace.toString();
        if (MapUtils.isEmpty(tableNames)) {
            return false;
        }
        Map<String, Table> tables = tableNames.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), metadataProxy.getTable(customer, entry.getValue())))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        return MapUtils.isNotEmpty(tables) && tables.values().stream().noneMatch(Objects::isNull);
    }
}
