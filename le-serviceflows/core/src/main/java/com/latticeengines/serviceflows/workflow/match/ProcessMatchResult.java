package com.latticeengines.serviceflows.workflow.match;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
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
public class ProcessMatchResult extends RunSparkJob<ProcessMatchResultConfiguration, ParseMatchResultJobConfig> {

    private static final Logger log = LoggerFactory.getLogger(ProcessMatchResult.class);

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

        try {
            String candidateDir = PathUtils.toParquetOrAvroDir(matchCommand.getCandidateLocation());
            if (StringUtils.isNotBlank(candidateDir) && HdfsUtils.isDirectory(yarnConfiguration, candidateDir)) {
                Table candidateTable = MetadataConverter.getTable(yarnConfiguration, candidateDir);
                HdfsDataUnit candidatesUnit = new HdfsDataUnit();
                dataUnit.setName("MatchCandidates");
                dataUnit.setPath(candidateDir);
                dataUnit.setCount(candidateTable.getExtracts().get(0).getPid());
                String candidateTableName = NamingUtils.timestamp("MatchCandidates");
                candidateTable = toTable(candidateTableName, candidatesUnit);
                metadataProxy.createTable(configuration.getCustomer(), candidateTableName, candidateTable);
                putStringValueInContext(MATCH_CANDIDATES_TABLE_NAME, candidateTableName);
            }
        } catch (Exception e) {
            log.warn("Failed to extract match candidates table.", e);
        }

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
        putStringValueInContext(MATCH_RESULT_TABLE_NAME, eventTable.getName());

        Table upstreamTable = getObjectFromContext(PREMATCH_UPSTREAM_EVENT_TABLE, Table.class);
        if (upstreamTable != null) {
            metadataProxy.deleteTable(customer, upstreamTable.getName());
        }
        removeObjectFromContext(PREMATCH_UPSTREAM_EVENT_TABLE);
        setMatchRate(result);

    }

    private void setMatchRate(SparkJobResult result) {
        long matchCount = 0L;
        long publicCount = 0L;
        long totalCount = 0L;
        String output = result.getOutput();
        if (StringUtils.isNotBlank(output)) {
            Map<String, Long> report = JsonUtils.deserialize(output, new TypeReference<Map<String, Long>>() {});
            matchCount = report.getOrDefault("MatchCount", 0L);
            publicCount = report.getOrDefault("PublicDomainCount", 0L);
            totalCount = report.getOrDefault("TotalCount", 0L);
        }
        matchCount += hasKeyInContext(MATCH_RESULT_MATCH_COUNT) ? getLongValueFromContext(MATCH_RESULT_MATCH_COUNT) : 0;
        publicCount += hasKeyInContext(MATCH_RESULT_PUBLIC_DOMAIN_COUNT) ? getLongValueFromContext(MATCH_RESULT_PUBLIC_DOMAIN_COUNT) : 0;
        totalCount += hasKeyInContext(MATCH_RESULT_TOTAL_COUNT) ? getLongValueFromContext(MATCH_RESULT_TOTAL_COUNT) : 0;
        putLongValueInContext(MATCH_RESULT_MATCH_COUNT, matchCount);
        putLongValueInContext(MATCH_RESULT_PUBLIC_DOMAIN_COUNT, publicCount);
        putLongValueInContext(MATCH_RESULT_TOTAL_COUNT, totalCount);

        Double matchRate = 1.0D * matchCount / totalCount;
        Double publicDomainRate = 1.0D * publicCount / totalCount;
        putDoubleValueInContext(MATCH_RESULT_MATCH_RATE, matchRate);
        putDoubleValueInContext(MATCH_RESULT_PUBLIC_DOMAIN_RATE, publicDomainRate);
        log.info("MatchRate=" + getDoubleValueFromContext(MATCH_RESULT_MATCH_RATE));
        log.info("PublicDomainRate=" + getDoubleValueFromContext(MATCH_RESULT_PUBLIC_DOMAIN_RATE));
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
