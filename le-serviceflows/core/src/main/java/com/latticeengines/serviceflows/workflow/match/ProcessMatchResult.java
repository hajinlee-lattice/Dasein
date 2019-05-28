package com.latticeengines.serviceflows.workflow.match;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.ParseMatchResultJobConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.match.ParseMatchResultJob;

@Component("processMatchResult")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessMatchResult extends RunSparkJob<ProcessMatchResultConfiguration, //
        ParseMatchResultJobConfig, ParseMatchResultJob> {

    @Inject
    private MetadataProxy metadataProxy;

    private Table matchResultTable;
    private Table preMatchTable;

    @Override
    protected Class<ParseMatchResultJob> getJobClz() {
        return ParseMatchResultJob.class;
    }

    @Override
    protected ParseMatchResultJobConfig configureJob(ProcessMatchResultConfiguration stepConfiguration) {
        MatchCommand matchCommand = getObjectFromContext(MATCH_COMMAND, MatchCommand.class);
        String inputDir = matchCommand.getResultLocation();
        if (StringUtils.isBlank(inputDir)) {
            throw new RuntimeException("Cannot find match result dir.");
        }

        matchResultTable = MetadataConverter.getTable(yarnConfiguration, inputDir);
        HdfsDataUnit dataUnit = new HdfsDataUnit();
        dataUnit.setName("matchResult");
        dataUnit.setPath(inputDir);
        dataUnit.setCount(matchCommand.getRowsRequested().longValue());
        List<DataUnit> inputUnits = new ArrayList<>();
        inputUnits.add(dataUnit);

        preMatchTable = getObjectFromContext(PREMATCH_EVENT_TABLE, Table.class);
        if (preMatchTable != null) {
            log.info("PreMatchTable=" + JsonUtils.serialize(preMatchTable));
            HdfsDataUnit eventTable = preMatchTable.toHdfsDataUnit("eventTable");
            eventTable.setCount(matchCommand.getRowsRequested().longValue());
            inputUnits.add(eventTable);
        }
        log.info("InputUnits=" + JsonUtils.serialize(inputUnits));
        ParseMatchResultJobConfig jobConfig = new ParseMatchResultJobConfig();
        jobConfig.setInput(inputUnits);
        jobConfig.sourceColumns = sourceCols(preMatchTable);
        jobConfig.excludeDataCloudAttrs = getConfiguration().isExcludeDataCloudAttrs();
        jobConfig.keepLid = getConfiguration().isKeepLid();
        jobConfig.matchGroupId = getConfiguration().getMatchGroupId();
        jobConfig.joinInternalId = getConfiguration().isJoinInternalId();
        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String customer = configuration.getCustomer();
        String eventTableName = NamingUtils.timestampWithRandom("MatchDataCloud");
        Table eventTable = toTable(eventTableName, result.getTargets().get(0));
        overlayMetadata(eventTable);
        metadataProxy.createTable(customer, eventTableName, eventTable);
        putObjectInContext(EVENT_TABLE, eventTable);
        putObjectInContext(MATCH_RESULT_TABLE, eventTable);

        Table upstreamTable = getObjectFromContext(PREMATCH_UPSTREAM_EVENT_TABLE, Table.class);
        if (upstreamTable != null) {
            metadataProxy.deleteTable(customer, upstreamTable.getName());
        }
        removeObjectFromContext(PREMATCH_UPSTREAM_EVENT_TABLE);
    }

    private void overlayMetadata(Table eventTable) {
        Map<String, Attribute> attributeMap = new HashMap<>();
        matchResultTable.getAttributes().forEach(attr -> attributeMap.put(attr.getName(), attr));
        if (preMatchTable != null) {
            preMatchTable.getAttributes().forEach(attr -> attributeMap.put(attr.getName(), attr));
        }
        super.overlayTableSchema(eventTable, attributeMap);
    }

    private List<String> sourceCols(Table preMatchTable) {
        Table upstreamTable = getObjectFromContext(PREMATCH_UPSTREAM_EVENT_TABLE, Table.class);
        List<String> cols;
        if (upstreamTable != null) {
            cols = Arrays.asList(upstreamTable.getAttributeNames());
        } else {
            cols = Arrays.asList(preMatchTable.getAttributeNames());
        }
        log.info("Found source columns: " + StringUtils.join(cols, ", "));
        return cols;
    }


}
