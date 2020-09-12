package com.latticeengines.serviceflows.workflow.dataflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
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

    protected boolean tableInHdfs(Map<String, String> tableNames, boolean partitioned) {
        String customer = customerSpace.toString();
        Map<String, Table> tables = tableNames.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), metadataProxy.getTable(customer, entry.getValue())))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        return tableInHdfs(new ArrayList<>(tables.values()), partitioned);
    }

    protected boolean tableInHdfs(List<Table> tables, boolean partitioned) {
        return CollectionUtils.isNotEmpty(tables) && tables.stream().allMatch(table -> tableInHdfs(table, partitioned));
    }

    protected boolean tableInHdfs(Table table, boolean partitioned) {
        String tablePath = partitioned ? table.getExtracts().get(0).getPath() : table.getExtractsDirectory();
        boolean result = false;
        try {
            result = HdfsUtils.fileExists(yarnConfiguration, tablePath);
        } catch (IOException e) {
            log.warn("Failed to check if table exists with path {}", tablePath);
            e.printStackTrace();
        }
        if (!result) {
            log.warn("Found table {} in database but not in hdfs.", table.getName());
        }
        return result;
    }
}
