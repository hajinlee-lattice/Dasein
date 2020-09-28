package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportTimelineToS3StepConfiguration;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("exportTimelineToS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportTimelineToS3 extends BaseImportExportS3<ExportTimelineToS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportTimelineToS3.class);

    private List<String> s3ExportFilePaths = new ArrayList<>();

    private String CSV = "csv";

    @Value("${cdl.atlas.export.dropfolder.tag}")
    private String expire30dTag;

    @Value("${cdl.atlas.export.dropfolder.tag.value}")
    private String expire30dTagValue;

    @Value("${cdl.campaign.integration.session.context.ttl}")
    private long sessionContextTTLinSec;

    @Inject
    private S3Service s3Service;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        List<String> timelineExportTablePaths = getListObjectFromContext(TIMELINE_EXPORT_TABLES, String.class);
        if (timelineExportTablePaths == null || timelineExportTablePaths.isEmpty()) {
            return;
        }

        timelineExportTablePaths.forEach(hdfsFilePath -> {
            ImportExportRequest request = new ImportExportRequest();
            request.srcPath = hdfsFilePath;
            request.tgtPath = pathBuilder.convertAtlasFileExport(hdfsFilePath, podId, tenantId, dropBoxSummary,
                    exportS3Bucket);
            requests.add(request);
            // Collect all S3 FilePaths
            s3ExportFilePaths.add(request.tgtPath);
        });
        log.info("Uploaded S3 Files. tgtPath is {}", s3ExportFilePaths);
    }

    @Override
    public void execute() {
        super.execute();
        tagCreatedS3Objects();
    }

    private void tagCreatedS3Objects() {
        log.info("Tagging the created s3 files to expire in 30 days");
        s3ExportFilePaths.forEach(s3Path -> {
            try {
                s3Service.addTagToObject(s3Bucket, extractBucketLessPath(s3Path), expire30dTag, expire30dTagValue);
                log.info(String.format("Tagged %s to expire in 30 days", extractBucketLessPath(s3Path)));
            } catch (Exception e) {
                log.error(String.format("Failed to tag %s to expire in 30 days", s3Path));
            }
        });
    }

    private String extractBucketLessPath(String s3Path) {
        return s3Path.replace(pathBuilder.getProtocol() + pathBuilder.getProtocolSeparator()
                + pathBuilder.getPathSeparator() + s3Bucket + pathBuilder.getPathSeparator(), "");
    }

    @VisibleForTesting
    public void setS3ExportFiles(List<String> exportFiles) {
        s3ExportFilePaths = exportFiles;
    }
}
