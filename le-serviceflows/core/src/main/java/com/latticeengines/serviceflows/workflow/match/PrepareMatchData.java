package com.latticeengines.serviceflows.workflow.match;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.PrepareMatchDataJobConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.PrepareMatchDataConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.match.PrepareMatchDataJob;

@Component("prepareMatchData")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareMatchData extends RunSparkJob<PrepareMatchDataConfiguration, PrepareMatchDataJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(PrepareMatchData.class);

    private static final String LDC_PREPARE_MATCH_DATA = "PrepareMatchData";

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    protected Class<? extends AbstractSparkJob<PrepareMatchDataJobConfig>> getJobClz() {
        return PrepareMatchDataJob.class;
    }

    @Override
    protected PrepareMatchDataJobConfig configureJob(PrepareMatchDataConfiguration stepConfiguration) {
        Table preMatchEventTable = preMatchEventTable();
        putObjectInContext(PREMATCH_EVENT_TABLE, preMatchEventTable);
        List<String> matchFields = getMatchFields(preMatchEventTable);

        PrepareMatchDataJobConfig config = new PrepareMatchDataJobConfig();
        config.matchFields = matchFields;
        config.matchGroupId = configuration.getMatchGroupId();
        config.setInput(Collections.singletonList(preMatchEventTable.toHdfsDataUnit("preMatchEvent")));
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String targetTableName = NamingUtils.uuid(LDC_PREPARE_MATCH_DATA);
        HdfsDataUnit resultUnit = result.getTargets().get(0);
        log.info("Creating match data table {} with result data unit {}", targetTableName, JsonUtils.serialize(resultUnit));
        Table targetTable = toTable(targetTableName, resultUnit);
        putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, targetTable);
    }

    @Override
    public void skipStep() {
        log.info("Skip prepare matching data step.");
        log.info("Skip embedded bulk match workflow.");
        skipEmbeddedWorkflow(getParentNamespace(), "", BulkMatchWorkflowConfiguration.class);
    }

    private Table preMatchEventTable() {
        Table preMatchEventTable = getObjectFromContext(PREMATCH_UPSTREAM_EVENT_TABLE, Table.class);
        if (preMatchEventTable == null) {
            preMatchEventTable = metadataProxy.getTable(configuration.getCustomer(),
                    configuration.getInputTableName());
        }
        return preMatchEventTable;
    }

    private List<String> getMatchFields(Table preMatchEventTable) {
        List<String> fields = Arrays.asList(preMatchEventTable.getAttributeNames());

        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(fields);
        List<String> allFields = new ArrayList<>();
        // add all resolved match fields
        Arrays.stream(MatchKey.values()) //
                .map(keyMap::get) //
                .filter(CollectionUtils::isNotEmpty) //
                .forEach(allFields::addAll);
        addIfAbsent(fields, allFields, configuration.getMatchGroupId());
        addIfAbsent(fields, allFields, InterfaceName.Id.name());
        addIfAbsent(fields, allFields, InterfaceName.InternalId.name());
        if (configuration.isMapToLatticeAccount()) {
            addIfAbsent(fields, allFields, InterfaceName.CustomerAccountId.name());
        }
        log.info("Selected fields={}", String.join(",", allFields));
        return allFields;
    }

    private void addIfAbsent(List<String> fieldsToAdd, List<String> allFields, String fieldName) {
        if (fieldsToAdd.contains(fieldName) && !allFields.contains(fieldName)) {
            allFields.add(fieldName);
        }
    }
}
