package com.latticeengines.serviceflows.workflow.match;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.PrepareMatchDataParameters;
import com.latticeengines.domain.exposed.serviceflows.core.steps.PrepareMatchDataConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("prepareMatchDataStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareMatchDataStep extends RunDataFlow<PrepareMatchDataConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PrepareMatchDataStep.class);

    private static final String LDC_PAREPARE_MATCH_DATA = "PrepareMatchData";

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public void onConfigurationInitialized() {

        Table preMatchEventTable = preMatchEventTable();
        putObjectInContext(PREMATCH_EVENT_TABLE, preMatchEventTable);
        List<String> matchFields = getMatchFields(preMatchEventTable);

        PrepareMatchDataParameters parameters = new PrepareMatchDataParameters();
        parameters.sourceTableName = preMatchEventTable.getName();
        parameters.matchFields = matchFields;
        parameters.matchGroupId = configuration.getMatchGroupId();

        configuration.setDataFlowParams(parameters);
        String targetTableName = NamingUtils.uuid(LDC_PAREPARE_MATCH_DATA);
        configuration.setTargetTableName(targetTableName);

    }

    private List<String> getMatchFields(Table preMatchEventTable) {
        List<String> fields = Arrays.asList(preMatchEventTable.getAttributeNames());

        List<String> allFields = new ArrayList<>();
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(fields);
        for (MatchKey matchKey : MatchKey.values()) {
            if (CollectionUtils.isNotEmpty(keyMap.get(matchKey))) {
                allFields.addAll(keyMap.get(matchKey));
            }
        }
        String idColumn = configuration.getMatchGroupId();
        if (fields.contains(idColumn) && !allFields.contains(idColumn)) {
            allFields.add(idColumn);
        }
        if (fields.contains(InterfaceName.Id.name()) && !allFields.contains(InterfaceName.Id.name())) {
            allFields.add(InterfaceName.Id.name());
        }
        if (fields.contains(InterfaceName.InternalId.name()) && !allFields.contains(InterfaceName.InternalId.name())) {
            allFields.add(InterfaceName.InternalId.name());
        }
        log.info("Selected fields=" + String.join(",", allFields));
        return allFields;
    }

    @Override
    public void onExecutionCompleted() {
        Table targetTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
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
            preMatchEventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                    configuration.getInputTableName());
        }
        return preMatchEventTable;
    }

}
