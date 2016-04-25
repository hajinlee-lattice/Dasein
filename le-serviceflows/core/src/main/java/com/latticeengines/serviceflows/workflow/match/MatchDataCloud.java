package com.latticeengines.serviceflows.workflow.match;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.propdata.match.IOBufferType;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("matchDataCloud")
public class MatchDataCloud extends BaseWorkflowStep<MatchStepConfiguration> {

    private static final Log log = LogFactory.getLog(MatchDataCloud.class);
    static final String LDC_MATCH = "DataCloudMatch";

    @Autowired
    private MatchProxy matchProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    private MatchCommand matchCommand;

    @Override
    public void execute() {
        log.info("Inside MatchDataCloud execute()");
        Table preMatchEventTable = preMatchEventTable();
        executionContext.putString(PREMATCH_EVENT_TABLE, JsonUtils.serialize(preMatchEventTable));
        match(preMatchEventTable);
        executionContext.putString(MATCH_ROOT_UID, matchCommand.getRootOperationUid());
        Table matchResultTable = createMatchResultTable();
        executionContext.putString(MATCH_RESULT_TABLE, JsonUtils.serialize(matchResultTable));
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
        waitForMatchCommand();    }

    private MatchInput prepareMatchInput(Table preMatchEventTable) {
        MatchInput matchInput = new MatchInput();
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.DerivedColumns);
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
        matchInput.setInputBuffer(inputBuffer);

        return matchInput;
    }

    private void waitForMatchCommand() {
        String rootUid = matchCommand.getRootOperationUid();
        log.info(String.format("Waiting for match command %s to complete", rootUid));

        MatchStatus status = null;
        do {
            matchCommand = matchProxy.bulkMatchStatus(rootUid);
            status = matchCommand.getMatchStatus();
            if (status == null) {
                throw new LedpException(LedpCode.LEDP_28024, new String[] { rootUid });
            }
            String logMsg = "Match Status = " + status;
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
