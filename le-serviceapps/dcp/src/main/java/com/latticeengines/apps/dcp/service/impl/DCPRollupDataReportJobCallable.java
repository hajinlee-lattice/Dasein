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
import org.springframework.data.domain.PageRequest;

import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.apps.dcp.entitymgr.DataReportEntityMgr;
import com.latticeengines.apps.dcp.provision.impl.DCPComponent;
import com.latticeengines.apps.dcp.workflow.DCPDataReportWorkflowSubmitter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DCPReportRequest;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

public class DCPRollupDataReportJobCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(DCPRollupDataReportJobCallable.class);

    private static final long TIME_PERIOD = TimeUnit.HOURS.toMillis(16L);

    private static final int LIMIT = 4;

    private static final int PAGE_SIZE = 10;

    private String jobArguments;

    private DataReportEntityMgr dataReportEntityMgr;

    private DCPDataReportWorkflowSubmitter dcpDataReportWorkflowSubmitter;

    private ZKConfigService zkConfigService;

    public DCPRollupDataReportJobCallable(Builder builder) {
        this.jobArguments = builder.jobArguments;
        this.dataReportEntityMgr = builder.dataReportEntityMgr;
        this.dcpDataReportWorkflowSubmitter = builder.dcpDataReportWorkflowSubmitter;
        this.zkConfigService = builder.zkConfigService;
    }

    @Override
    public Boolean call() throws Exception {
        log.info("begin to rollup tenant level data report");
        int pageIndex = 0;
        int number = 0;
        List<Pair<String, Date>> ownerIdToDate = null;
        do {
            PageRequest pageRequest = PageRequest.of(pageIndex, PAGE_SIZE);
            ownerIdToDate = dataReportEntityMgr.getOwnerIdAndTime(DataReportRecord.Level.Tenant, "refreshTime",
                    pageRequest);
            log.info("data is " + JsonUtils.serialize(ownerIdToDate));
            if (CollectionUtils.isNotEmpty(ownerIdToDate)) {
                long currentTime = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

                for (Pair<String, Date> pair : ownerIdToDate) {
                    String ownerId = pair.getLeft();
                    Date refreshDate = pair.getRight();
                    CustomerSpace space = CustomerSpace.parse(ownerId);
                    // check the config in zk
                    Boolean disableRollup = zkConfigService.isRollupDisabled(space, DCPComponent.componentName);
                    if (Boolean.TRUE.equals(disableRollup)) {
                        log.info("disable rollup for tenant {}", ownerId);
                        continue;
                    }
                    if (currentTime - refreshDate.getTime() > TIME_PERIOD) {
                        DCPReportRequest request = new DCPReportRequest();
                        request.setRootId(ownerId);
                        request.setLevel(DataReportRecord.Level.Tenant);
                        request.setMode(DataReportMode.RECOMPUTE_TREE);
                        ApplicationId appId = dcpDataReportWorkflowSubmitter.submit(CustomerSpace.parse(ownerId), request,
                                new WorkflowPidWrapper(-1L));
                        log.info("ownerId {}, refresh time {}, current time {}, the appId {}", ownerId, refreshDate,
                                currentTime, appId);
                        number++;
                        if (number >= LIMIT) {
                            break;
                        }
                    }
                }
            }
            pageIndex++;
        } while(number < LIMIT && CollectionUtils.size(ownerIdToDate) == PAGE_SIZE);

        log.info("page index {}, the rollup number is {}", pageIndex, number);
        return null;
    }

    public static class Builder {
        private String jobArguments;

        private DataReportEntityMgr dataReportEntityMgr;

        private DCPDataReportWorkflowSubmitter dcpDataReportWorkflowSubmitter;

        private ZKConfigService zkConfigService;

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

        public Builder zkConfigService(ZKConfigService zkConfigService) {
            this.zkConfigService = zkConfigService;
            return this;
        }

        public Builder dcpDataReportWorkflowSubmitter(DCPDataReportWorkflowSubmitter dcpDataReportWorkflowSubmitter) {
            this.dcpDataReportWorkflowSubmitter = dcpDataReportWorkflowSubmitter;
            return this;
        }
    }
}
