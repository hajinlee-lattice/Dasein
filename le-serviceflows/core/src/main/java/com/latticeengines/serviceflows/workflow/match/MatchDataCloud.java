package com.latticeengines.serviceflows.workflow.match;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.IOBufferType;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("matchDataCloud")
public class MatchDataCloud extends BaseWorkflowStep<MatchStepConfiguration> {

    private static final Log log = LogFactory.getLog(MatchDataCloud.class);
    static final String LDC_MATCH = "DataCloudMatch";

    @Autowired
    private MatchProxy matchProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    private MatchCommand matchCommand;

    @Override
    public void execute() {
        log.info("Inside MatchDataCloud execute()");
        Table preMatchEventTable = preMatchEventTable();
        putObjectInContext(PREMATCH_EVENT_TABLE, preMatchEventTable);
        match(preMatchEventTable);
        putStringValueInContext(MATCH_ROOT_UID, matchCommand.getRootOperationUid());
        Table matchResultTable = createMatchResultTable();
        putObjectInContext(MATCH_RESULT_TABLE, matchResultTable);
    }

    private Table preMatchEventTable() {
        Table preMatchEventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getInputTableName());
        preMatchEventTable.setName(preMatchEventTable.getName() + "_" + System.currentTimeMillis());
        return preMatchEventTable;
    }

    private void match(Table preMatchEventTable) {
        MatchInput input = prepareMatchInput(preMatchEventTable);
        matchCommand = matchProxy.matchBulk(input, "");
        configContext();
        waitForMatchCommand();
    }

    private void configContext() {
        putStringValueInContext(MATCH_IS_CASCADING_FLOW, matchCommand.getCascadingFlow());
    }

    private MatchInput prepareMatchInput(Table preMatchEventTable) {
        MatchInput matchInput = new MatchInput();
        matchInput.setYarnQueue(getConfiguration().getMatchQueue());
        matchInput.setTableName(preMatchEventTable.getName());
        
        if (getConfiguration().getCustomizedColumnSelection() == null
                && getConfiguration().getPredefinedColumnSelection() == null) {
            throw new RuntimeException("Must specify either CustomizedColumnSelection or PredefinedColumnSelection");
        }

        Predefined predefined = getConfiguration().getPredefinedColumnSelection();
        if (predefined != null) {
            matchInput.setPredefinedSelection(predefined);
            String version = getConfiguration().getPredefinedSelectionVersion();
            if (StringUtils.isEmpty(version)) {
                version = "1.0";
                getConfiguration().setPredefinedSelectionVersion(version);
            }
            matchInput.setPredefinedVersion(version);

            putStringValueInContext(MATCH_PREDEFINED_SELECTION, predefined.getName());
            putStringValueInContext(MATCH_PREDEFINED_SELECTION_VERSION, version);

            log.info("Using predefined column selection " + predefined + " at version " + version);
        } else {
            matchInput.setCustomSelection(getConfiguration().getCustomizedColumnSelection());

            putObjectInContext(MATCH_CUSTOMIZED_SELECTION, getConfiguration().getCustomizedColumnSelection());

        }
        matchInput.setDataCloudVersion(getConfiguration().getDataCloudVersion());
        log.info("Using Data Cloud Version = " + getConfiguration().getDataCloudVersion());
        
        matchInput.setTenant(new Tenant(configuration.getCustomerSpace().toString()));
        matchInput.setOutputBufferType(IOBufferType.AVRO);

        switch (configuration.getMatchJoinType()) {
        case INNER_JOIN:
            matchInput.setReturnUnmatched(false);
            break;
        case OUTER_JOIN:
            matchInput.setReturnUnmatched(true);
            break;
        default:
            throw new UnsupportedOperationException("Unknown join type " + configuration.getMatchJoinType());
        }

        String avroDir = ExtractUtils.getSingleExtractPath(yarnConfiguration, preMatchEventTable);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);

        Schema providedSchema;
        try {
            providedSchema = TableUtils.createSchema(preMatchEventTable.getName(), preMatchEventTable);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create avro schema from pre-match event table.", e);
        }

        Schema extractedSchema;
        try {
            String avroGlob;
            if (avroDir.endsWith(".avro")) {
                avroGlob = avroDir;
            } else {
                avroGlob = avroDir.endsWith("/") ? avroDir + "*.avro" : avroDir + "/*.avro";
            }
            extractedSchema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract avro schema from input avro.", e);
        }
        
        Schema schema = AvroUtils.alignFields(providedSchema, extractedSchema);

        inputBuffer.setSchema(schema);

        matchInput.setInputBuffer(inputBuffer);

        matchInput.setExcludePublicDomains(getConfiguration().isExcludePublicDomains());

        return matchInput;
    }

    private void waitForMatchCommand() {
        String rootUid = matchCommand.getRootOperationUid();
        String appId = matchCommand.getApplicationId();
        if (StringUtils.isEmpty(appId)) {
            appId = "null";
        }
        log.info(String.format("Waiting for match command %s [ApplicationId=%s] to complete", rootUid, appId));

        MatchStatus status = null;
        do {
            matchCommand = matchProxy.bulkMatchStatus(rootUid);
            status = matchCommand.getMatchStatus();
            if (status == null) {
                throw new LedpException(LedpCode.LEDP_28024, new String[] { rootUid });
            }
            appId = matchCommand.getApplicationId();
            if (StringUtils.isEmpty(appId)) {
                appId = "null";
            }
            String logMsg = "[ApplicationId=" + appId + "] Match Status = " + status;
            if (MatchStatus.MATCHING.equals(status)) {
                Float progress = matchCommand.getProgress();
                logMsg += String.format(": %.2f %%", progress * 100);
            }
            log.info(logMsg);

            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                // Ignore InterruptedException
            }

        } while (!status.isTerminal());

        if (!MatchStatus.FINISHED.equals(status)) {
            throw new IllegalStateException(
                    "The terminal status of match is " + status + " instead of " + MatchStatus.FINISHED);
        }

    }

    private Table createMatchResultTable() {
        Table matchResultTable = MetadataConverter.getTable(yarnConfiguration, matchCommand.getResultLocation(), null,
                null);
        String resultTableName = LDC_MATCH + "_" + matchCommand.getRootOperationUid();
        matchResultTable.setName(resultTableName);
        metadataProxy.createTable(configuration.getCustomerSpace().toString(), resultTableName, matchResultTable);

        try {
            // wait 3 seconds for metadata to create the table
            Thread.sleep(3000L);
        } catch (InterruptedException e) {
            // ignore
        }
        return matchResultTable;
    }

}
