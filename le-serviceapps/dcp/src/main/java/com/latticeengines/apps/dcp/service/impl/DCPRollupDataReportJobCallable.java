package com.latticeengines.apps.dcp.service.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.dcp.entitymgr.DataReportEntityMgr;
import com.latticeengines.apps.dcp.workflow.DCPDataReportWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DCPReportRequest;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

public class DCPRollupDataReportJobCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(DCPRollupDataReportJobCallable.class);

    private static final long timePeriod = TimeUnit.HOURS.toMillis(16L);

    private String jobArguments;

    private DataReportEntityMgr dataReportEntityMgr;

    private DCPDataReportWorkflowSubmitter dcpDataReportWorkflowSubmitter;

    public DCPRollupDataReportJobCallable(Builder builder) {
        this.jobArguments = builder.jobArguments;
        this.dataReportEntityMgr = builder.dataReportEntityMgr;
        this.dcpDataReportWorkflowSubmitter = builder.dcpDataReportWorkflowSubmitter;
    }

    @Override
    public Boolean call() throws Exception {
        log.info("begin to rollup tenant level data report");
        List<Pair<String, Date>> ownerIdToDate =
                dataReportEntityMgr.getOwnerIdAndTime(DataReportRecord.Level.Tenant, "REFRESH_TIME",4);
        if (CollectionUtils.isNotEmpty(ownerIdToDate)) {
            long currentTime = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            for (Pair<String, Date> pair : ownerIdToDate) {
                String ownerId = pair.getLeft();
                Date refreshDate = pair.getRight();
                if (currentTime - refreshDate.getTime() > timePeriod) {
                    DCPReportRequest request = new DCPReportRequest();
                    request.setRootId(ownerId);
                    request.setLevel(DataReportRecord.Level.Tenant);
                    request.setMode(DataReportMode.RECOMPUTE_TREE);
                    ApplicationId appId = dcpDataReportWorkflowSubmitter.submit(CustomerSpace.parse(ownerId), request,
                            new WorkflowPidWrapper(-1L));
                    log.info("refresh time {}, current time {}, the appId is {}", refreshDate, currentTime, appId);
                }
            }
        }
        return null;
    }

    public static class Builder {
        private String jobArguments;

        private DataReportEntityMgr dataReportEntityMgr;

        private DCPDataReportWorkflowSubmitter dcpDataReportWorkflowSubmitter;

        public Builder() {
        }

        public Builder jobArguments(String jobArguments) {
            this.jobArguments = jobArguments;
            return this;
        }

        public Builder dataReportEntityMgr(DataReportEntityMgr dataReportEntityMgr) {
            this.dataReportEntityMgr = dataReportEntityMgr;
            return this;
        }

        public Builder dcpDataReportWorkflowSubmitter(DCPDataReportWorkflowSubmitter dcpDataReportWorkflowSubmitter) {
            this.dcpDataReportWorkflowSubmitter = dcpDataReportWorkflowSubmitter;
            return this;
        }
    }
}
