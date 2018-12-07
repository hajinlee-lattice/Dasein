package com.latticeengines.serviceflows.workflow.match;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.ParseMatchResultParameters;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultCascadingConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("processMatchResultCascading")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessMatchResultCascading extends RunDataFlow<ProcessMatchResultCascadingConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessMatchResultCascading.class);

    @Inject
    private MetadataProxy metadataProxy;

    private static final String EVENT = "DataCloudMatchEvent";
    private static final String LDC_MATCH = "DataCloudMatch";
    private String resultTableName;

    @Override
    public void onConfigurationInitialized() {
        ProcessMatchResultCascadingConfiguration configuration = getConfiguration();
        Table matchResultTable = createMatchResultTable();
        Table preMatchTable = getObjectFromContext(PREMATCH_EVENT_TABLE, Table.class);
        resultTableName = matchResultTable.getName();
        String eventTableName = resultTableName.replaceFirst(LDC_MATCH, EVENT);
        configuration.setTargetTableName(eventTableName);
        ParseMatchResultParameters parameters = new ParseMatchResultParameters();
        parameters.matchTableName = matchResultTable.getName();
        parameters.sourceTableName = preMatchTable.getName();
        parameters.sourceColumns = sourceCols(preMatchTable);
        parameters.excludeDataCloudAttrs = configuration.isExcludeDataCloudAttrs();
        parameters.keepLid = configuration.isKeepLid();
        parameters.matchGroupId = configuration.getMatchGroupId();
        parameters.joinInternalId = configuration.isJoinInternalId();
        configuration.setDataFlowParams(parameters);
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(EVENT_TABLE, eventTable);
        putObjectInContext(MATCH_RESULT_TABLE, eventTable);
        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), resultTableName);

        Table upstreamTable = getObjectFromContext(PREMATCH_UPSTREAM_EVENT_TABLE, Table.class);
        if (upstreamTable != null) {
            metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), upstreamTable.getName());
        }
        removeObjectFromContext(PREMATCH_UPSTREAM_EVENT_TABLE);
    }

    private Table createMatchResultTable() {
        MatchCommand matchCommand = getObjectFromContext(MATCH_COMMAND, MatchCommand.class);
        Table matchResultTable = MetadataConverter.getTable(yarnConfiguration, matchCommand.getResultLocation(), null,
                null);
        String resultTableName = NamingUtils.uuid(LDC_MATCH, UUID.fromString(matchCommand.getRootOperationUid()));
        matchResultTable.setName(resultTableName);
        String customerSpace = configuration.getCustomerSpace().toString();
        metadataProxy.createTable(customerSpace, resultTableName, matchResultTable);
        return matchResultTable;
    }

    private List<String> sourceCols(Table preMatchTable) {
        Table upstreamTable = getObjectFromContext(PREMATCH_UPSTREAM_EVENT_TABLE, Table.class);
        if (upstreamTable != null) {
            List<String> cols = Arrays.asList(upstreamTable.getAttributeNames());
            log.info("Found source columns: " + StringUtils.join(cols, ", "));
            return cols;
        }
        List<String> cols = Arrays.asList(preMatchTable.getAttributeNames());
        // String idCol = getIdColumn(preMatchTable);
        // cols.add(idCol);
        // for (Attribute attr : preMatchTable.getAttributes()) {
        // if (!idCol.equalsIgnoreCase(attr.getName())) {
        // cols.add(attr.getName());
        // }
        // }
        log.info("Found source columns: " + StringUtils.join(cols, ", "));
        return cols;
    }
}
