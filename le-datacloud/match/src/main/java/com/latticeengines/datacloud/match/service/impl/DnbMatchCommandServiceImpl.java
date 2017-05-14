package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.concurrent.TimeUnit;

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

    @Autowired
    private DnbMatchCommandEntityMgr dnbMatchEntityMgr;

    @Override
    public void dnbMatchCommandCreate(DnBBatchMatchContext dnbMatchContext) {
        DnBMatchCommand dnbMatchCommand = new DnBMatchCommand();
        dnbMatchCommand.copyContextData(dnbMatchContext);
        dnbMatchEntityMgr.createCommand(dnbMatchCommand);
    }

    @Override
    public void dnbMatchCommandUpdate(DnBBatchMatchContext dnbMatchContext) {
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
        dnbMatchCommand.setUnmatchedRecords(unmatchedRecords);
        dnbMatchCommand.setAcceptedRecords(acceptedRecords);
        dnbMatchCommand.setDiscardedRecords(discardedRecords);
        dnbMatchCommand.setFinishTime(new Date());
        if (dnbMatchCommand.getStartTime() != null && dnbMatchCommand.getFinishTime() != null)
            dnbMatchCommand.setDuration((int) (TimeUnit.MILLISECONDS
                    .toMinutes(dnbMatchCommand.getFinishTime().getTime() - dnbMatchCommand.getStartTime().getTime())));
        dnbMatchEntityMgr.updateCommand(dnbMatchCommand);
    }

    @Override
    public DnBMatchCommand findRecordByField(String field, Object value) {
        return dnbMatchEntityMgr.findRecordByField(field, value);
    }

    @Override
    public void dnbMatchCommandDelete(DnBMatchCommand dnbMatchCommand) {
        dnbMatchEntityMgr.deleteCommand(dnbMatchCommand);
    }

}
