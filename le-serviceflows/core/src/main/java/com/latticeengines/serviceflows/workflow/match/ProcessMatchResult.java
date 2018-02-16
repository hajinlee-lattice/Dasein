package com.latticeengines.serviceflows.workflow.match;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.ParseMatchResultParameters;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("processMatchResult")
public class ProcessMatchResult extends RunDataFlow<ProcessMatchResultConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessMatchResult.class);

    @Inject
    private MetadataProxy metadataProxy;

    private static final String EVENT = "DataCloudMatchEvent";
    private static final String LDC_MATCH = "DataCloudMatch";
    private String resultTableName;

    @Override
    public void onConfigurationInitialized() {
        ProcessMatchResultConfiguration configuration = getConfiguration();
        Table matchResultTable = createMatchResultTable();
        Table preMatchTable = getObjectFromContext(PREMATCH_EVENT_TABLE, Table.class);
        resultTableName = matchResultTable.getName();
        String eventTableName = resultTableName.replaceFirst(LDC_MATCH, EVENT);
        configuration.setTargetTableName(eventTableName);
        ParseMatchResultParameters parameters = new ParseMatchResultParameters();
        parameters.sourceTableName = matchResultTable.getName();
        parameters.sourceColumns = sourceCols(preMatchTable);
        parameters.excludeDataCloudAttrs = configuration.isExcludeDataCloudAttrs();
        configuration.setDataFlowParams(parameters);

    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(EVENT_TABLE, eventTable);
        putObjectInContext(MATCH_RESULT_TABLE, eventTable);
        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), resultTableName);
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
        List<String> cols = Arrays.asList(preMatchTable.getAttributeNames());
//        String idCol = getIdColumn(preMatchTable);
//        cols.add(idCol);
//        for (Attribute attr : preMatchTable.getAttributes()) {
//            if (!idCol.equalsIgnoreCase(attr.getName())) {
//                cols.add(attr.getName());
//            }
//        }
        log.info("Found source columns: " + StringUtils.join(cols, ", "));
        return cols;
    }

    private String getIdColumn(Table table) {
        String idAttr = getStringValueFromContext(MATCH_INPUT_ID_COLUMN);
        if (StringUtils.isBlank(idAttr)) {
            List<Attribute> idColumns = table.getAttributes(LogicalDataType.InternalId);
            if (idColumns.isEmpty()) {
                if (table.getAttribute("Id") == null) {
                    throw new RuntimeException("No Id columns found in prematch table");
                } else {
                    log.warn("No column with LogicalDataType InternalId in prematch table.  Choosing column called \"Id\"");
                    idColumns.add(table.getAttribute("Id"));
                }
            }
            if (idColumns.size() != 1) {
                log.warn(String.format("Multiple id columns in prematch table.  Choosing %s", idColumns.get(0).getName()));
            }
            return idColumns.get(0).getName();
        } else {
            return idAttr;
        }
    }

}
