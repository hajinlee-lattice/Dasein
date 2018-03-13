package com.latticeengines.apps.cdl.workflow;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventModelingWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.SegmentExportUtil;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component
public class CustomEventModelingWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(CustomEventModelingWorkflowSubmitter.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private SegmentProxy segmentProxy;

    public ApplicationId submit(String segmentName) {

        CustomEventModelingWorkflowConfiguration configuration = generateConfiguration(segmentName);
        return workflowJobService.submit(configuration);
    }

    private CustomEventModelingWorkflowConfiguration generateConfiguration(String segmentName) {

        return new CustomEventModelingWorkflowConfiguration.Builder() //
                .metadataSegmentExport(createMetadataSegmentExport(segmentName)) //
                .build();
    }

    private MetadataSegmentExport createMetadataSegmentExport(String segmentName) {
        if (StringUtils.isEmpty(segmentName)) {
            return null;
        }
        CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId());
        MetadataSegment segment = segmentProxy.getMetadataSegmentByName(customerSpace.toString(), segmentName);
        MetadataSegmentExport metadataSegmentExport = new MetadataSegmentExport();
        metadataSegmentExport.setAccountFrontEndRestriction(segment.getAccountFrontEndRestriction());
        metadataSegmentExport.setContactFrontEndRestriction(segment.getContactFrontEndRestriction());

        metadataSegmentExport.setType(MetadataSegmentExportType.ACCOUNT_ID);
        metadataSegmentExport.setAccountFrontEndRestriction(new FrontEndRestriction());
        String exportedFileName = SegmentExportUtil.constructFileName(metadataSegmentExport.getExportPrefix(), null,
                metadataSegmentExport.getType());
        metadataSegmentExport.setFileName(exportedFileName);
        Table segmentExportTable = SegmentExportUtil.constructSegmentExportTable(MultiTenantContext.getTenant(),
                metadataSegmentExport.getType(), exportedFileName);

        segmentExportTable = metadataProxy.getTable(customerSpace.toString(), segmentExportTable.getName());
        metadataSegmentExport.setTableName(segmentExportTable.getName());

        String path = PathBuilder.buildDataFileUniqueExportPath(CamilleEnvironment.getPodId(), customerSpace)
                .toString();
        metadataSegmentExport.setPath(path);
        return metadataSegmentExport;
    }
}
