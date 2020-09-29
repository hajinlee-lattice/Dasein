package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.TimelineExportRequest;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.TimelineExportWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.TimeLineProxy;

@Component
public class TimelineExportWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(TimelineExportWorkflowSubmitter.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;
    @Inject
    private SegmentProxy segmentProxy;
    @Inject
    private TimeLineProxy timelineProxy;

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull String customerSpace, @NotNull TimelineExportRequest request,
                                @NotNull WorkflowPidWrapper pidWrapper) {
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
        Table latticeAccountTable = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.LatticeAccount,
                activeVersion);
        if (latticeAccountTable == null) {
            throw new IllegalArgumentException(String.format("Can't find latticeAccountTable, Failed to submit %s's TimelineExportWorkflow",
                    customerSpace));
        }
        // timelineId -> table name of timeline master store in active version
        Map<String, String> timelineTableNames = dataCollectionProxy.getTableNamesWithSignatures(customerSpace,
                TableRoleInCollection.TimelineProfile,
                activeVersion, null);
        if (MapUtils.isEmpty(timelineTableNames)) {
            throw new IllegalArgumentException(String.format("Can't find TimelineTable, Failed to submit %s's " +
                            "TimelineExportWorkflow", customerSpace));
        }

        List<TimeLine> timeLineList = timelineProxy.findAll(customerSpace);
        Map<String, String> accountTimelineTableNames = new HashMap<>();
        for (TimeLine timeLine : timeLineList) {
            if (BusinessEntity.Account.name().equals(timeLine.getEntity()) && timelineTableNames.containsKey(timeLine.getTimelineId())) {
                accountTimelineTableNames.put(timeLine.getTimelineId(),
                        timelineTableNames.get(timeLine.getTimelineId()));
            }
        }
        if (MapUtils.isEmpty(accountTimelineTableNames)) {
            log.error("Can't find timeline item at Account level, customerSpace is {}.", customerSpace);
            throw new IllegalArgumentException(String.format("Can't find timeline item at Account level, " +
                    "customerSpace id {}", customerSpace));
        }
        TimelineExportWorkflowConfiguration configuration = generateConfig(customerSpace, request, accountTimelineTableNames
                , latticeAccountTable, activeVersion);
        return workflowJobService.submit(configuration, pidWrapper.getPid());
    }

    private TimelineExportWorkflowConfiguration generateConfig(String customerSpace, TimelineExportRequest request,
                                                               Map<String, String> timelineTableNames,
                                                               Table latticeAccountTable, DataCollection.Version version) {
        MetadataSegment metadataSegment = null;
        if (!StringUtils.isEmpty(request.getSegmentName())) {
            metadataSegment = segmentProxy.getMetadataSegmentByName(customerSpace,
                    request.getSegmentName());
            if (metadataSegment == null) {
                throw new IllegalArgumentException(String.format("Can't find Segment using segmentName %s.",
                        request.getSegmentName()));
            }
        }
        return new TimelineExportWorkflowConfiguration.Builder()
                .customer(CustomerSpace.parse(customerSpace))
                .setRequest(request)
                .setTimelineTableNames(timelineTableNames)
                .setVersion(version)
                .setSegment(metadataSegment)
                .build();
    }
}
