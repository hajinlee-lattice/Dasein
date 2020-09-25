package com.latticeengines.apps.cdl.workflow;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.TimelineExportRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.TimelineExportWorkflowConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;

public class TimelineExportWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(TimelineExportWorkflowSubmitter.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;
    @Inject
    private SegmentProxy segmentProxy;

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
                            "TimelineExportWorkflow",
                    customerSpace));
        }
        TimelineExportWorkflowConfiguration configuration = generateConfig(customerSpace, request, timelineTableNames
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
                .setLatticeAccountTable(latticeAccountTable)
                .setRequest(request)
                .setTimelineTableNames(timelineTableNames)
                .setVersion(version)
                .setSegment(metadataSegment)
                .build();
    }
}
