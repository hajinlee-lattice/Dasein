package com.latticeengines.serviceflows.workflow.export;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportSegmentExportToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportSegmentExportToS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ExportSegmentExportToS3.class);

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        ImportExportRequest request = new ImportExportRequest();
        String exportId = getConfiguration().getAtlasExportId();
        InternalResourceRestApiProxy internalResourceRestApiProxy = new InternalResourceRestApiProxy(
                getConfiguration().getInternalResourceHostPort());
        MetadataSegmentExport metadataSegmentExport = internalResourceRestApiProxy
                .getMetadataSegmentExport(getConfiguration().getCustomerSpace(), exportId);

        String filePath = metadataSegmentExport.getPath();
        filePath = filePath.substring(0, filePath.length() - 1);
        request.srcPath = filePath + "_" + metadataSegmentExport.getFileName();
        request.tgtPath = pathBuilder.convertAtlasFile(request.srcPath, podId, tenantId, s3Bucket);
        requests.add(request);
    }

}
