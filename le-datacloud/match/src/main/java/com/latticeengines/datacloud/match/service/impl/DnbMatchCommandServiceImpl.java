package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.entitymgr.DnbMatchCommandEntityMgr;
import com.latticeengines.datacloud.match.service.DnbMatchCommandService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DnBMatchCommand;

@Component("dnbMatchCommandService")
public class DnbMatchCommandServiceImpl implements DnbMatchCommandService {
    private static final Logger log = LoggerFactory.getLogger(DnbMatchCommandServiceImpl.class);

    @Autowired
    private DnbMatchCommandEntityMgr dnbMatchEntityMgr;

    @Override
    public void dnbMatchCommandCreate(DnBBatchMatchContext dnbMatchContext) {
        try {
            DnBMatchCommand dnbMatchCommand = new DnBMatchCommand();
            dnbMatchCommand.copyContextData(dnbMatchContext);
            dnbMatchEntityMgr.createCommand(dnbMatchCommand);
        } catch (Exception e) {
            log.error("Failed to create DnB match command in db: " + dnbMatchContext.getServiceBatchId(), e);
        }
    }

    @Override
    public void dnbMatchCommandUpdate(DnBBatchMatchContext dnbMatchContext) {
        try {
            if (dnbMatchContext.getServiceBatchId() == null) {
                return;
            }
            DnBMatchCommand dnbMatchCommand = dnbMatchEntityMgr.findRecordByField("BatchID",
                    dnbMatchContext.getServiceBatchId());
            // Initialization
            int size = dnbMatchContext.getContexts().values().size();
            Integer acceptedRecords = 0;
            Integer discardedRecords = 0;
            Integer unmatchedRecords = size;
            DnBReturnCode batchStatus = dnbMatchContext.getDnbCode();
            // DnB batch request is finished successfully
            if (batchStatus == DnBReturnCode.OK) {
                // checking all the contexts
                for (DnBMatchContext dnbContext : dnbMatchContext.getContexts().values()) {
                    DnBReturnCode status = dnbContext.getDnbCode();
                    if (status == DnBReturnCode.OK) {
                        acceptedRecords += 1;
                    } else if (status == DnBReturnCode.DISCARD) {
                        discardedRecords += 1;
                    }
                }
                unmatchedRecords = size - acceptedRecords - discardedRecords;
            }
            // Common fields for failed and successful DnB batch requests
            dnbMatchCommand.setDnbCode(batchStatus);
            dnbMatchCommand.setMessage(dnbMatchContext.getDnbCode().getMessage());
            dnbMatchCommand.setUnmatchedRecords(unmatchedRecords);
            dnbMatchCommand.setAcceptedRecords(acceptedRecords);
            dnbMatchCommand.setDiscardedRecords(discardedRecords);
            dnbMatchCommand.setFinishTime(new Date());
            if (dnbMatchCommand.getStartTime() != null && dnbMatchCommand.getFinishTime() != null)
                dnbMatchCommand.setDuration((int) (TimeUnit.MILLISECONDS.toMinutes(
                        dnbMatchCommand.getFinishTime().getTime() - dnbMatchCommand.getStartTime().getTime())));
            dnbMatchEntityMgr.updateCommand(dnbMatchCommand);
        } catch (Exception e) {
            log.error("Failed to update DnB match command in db: " + dnbMatchContext.getServiceBatchId(), e);
        }
    }

    // TODO: Consider to change to batch operation
    @Override
    public void dnbMatchCommandUpdateStatus(List<DnBBatchMatchContext> contexts) {
        contexts.forEach(context -> {
            DnBMatchCommand command = dnbMatchEntityMgr.findRecordByField("BatchID", context.getServiceBatchId());
            command.setDnbCode(context.getDnbCode());
            dnbMatchEntityMgr.updateCommand(command);
        });
    }

    @Override
    public DnBMatchCommand findRecordByField(String field, Object value) {
        return dnbMatchEntityMgr.findRecordByField(field, value);
    }

    @Override
    public void dnbMatchCommandDelete(DnBMatchCommand dnbMatchCommand) {
        dnbMatchEntityMgr.deleteCommand(dnbMatchCommand);
    }

    @Override
    public void finalize(String rootOperationUid) {
        dnbMatchEntityMgr.abandonCommands(rootOperationUid);
    }
}
