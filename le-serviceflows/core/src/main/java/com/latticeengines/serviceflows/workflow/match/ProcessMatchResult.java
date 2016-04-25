package com.latticeengines.serviceflows.workflow.match;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.match.ParseMatchResultParameters;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("processMatchResult")
public class ProcessMatchResult extends RunDataFlow<ProcessMatchResultConfiguration> {

    private static final Log log  = LogFactory.getLog(ProcessMatchResult.class);

    @Autowired
    private MetadataProxy metadataProxy;

    private static final String EVENT = "DataCloudMatchEvent";

    private String resultTableName;

    @Override
    public void onConfigurationInitialized() {
        ProcessMatchResultConfiguration configuration = getConfiguration();
        Table matchResultTable = JsonUtils.deserialize(executionContext.getString(MATCH_RESULT_TABLE), Table.class);
        Table preMatchTable = JsonUtils.deserialize(executionContext.getString(PREMATCH_EVENT_TABLE), Table.class);
        resultTableName = matchResultTable.getName();
        String eventTableName = resultTableName.replaceFirst(MatchDataCloud.LDC_MATCH, EVENT);
        configuration.setTargetTableName(eventTableName);
        ParseMatchResultParameters parameters = new ParseMatchResultParameters();
        parameters.sourceTableName = matchResultTable.getName();
        parameters.sourceColumns = sourceCols(preMatchTable);
        configuration.setDataFlowParams(parameters);
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), configuration.getTargetTableName());
        executionContext.putString(EVENT_TABLE, JsonUtils.serialize(eventTable));
        metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), resultTableName);
    }

    private List<String> sourceCols(Table preMatchTable) {
        List<String> cols = new ArrayList<>();
        String idCol = getIdColumn(preMatchTable);
        cols.add(idCol);
        for (Attribute attr : preMatchTable.getAttributes()) {
            cols.add(attr.getName());
        }
        log.info("Found source columns: " + StringUtils.join(cols, ", "));
        return cols;
    }

    private String getIdColumn(Table table) {
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
    }

}
