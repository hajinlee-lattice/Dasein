package com.latticeengines.serviceflows.workflow.export;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToS3StepConfiguration;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

@Component("exportSegmentExportToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportSegmentExportToS3 extends BaseExportToS3<ExportToS3StepConfiguration> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ExportSegmentExportToS3.class);

    @Override
    protected void buildRequests(List<ExportRequest> requests) {
        ExportRequest request = new ExportRequest();
        String exportId = getConfiguration().getMetadataSegmentExportId();
        InternalResourceRestApiProxy internalResourceRestApiProxy = new InternalResourceRestApiProxy(
                getConfiguration().getInternalResourceHostPort());
        MetadataSegmentExport metadataSegmentExport = internalResourceRestApiProxy
                .getMetadataSegmentExport(getConfiguration().getCustomerSpace(), exportId);

        String filePath = metadataSegmentExport.getPath();
        filePath = filePath.substring(0, filePath.length() - 1);
        request.srcDir = filePath + "_" + metadataSegmentExport.getFileName();
        request.tgtDir = pathBuilder.convertAtlasFile(request.srcDir, podId, tenantId, s3Bucket);
        requests.add(request);
    }

}
