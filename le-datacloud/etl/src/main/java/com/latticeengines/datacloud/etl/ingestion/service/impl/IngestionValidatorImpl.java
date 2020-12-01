package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.CronUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionValidator;
import com.latticeengines.datacloud.etl.utils.SftpUtils;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.ingestion.PatchBookConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.S3Configuration;
import com.latticeengines.domain.exposed.datacloud.ingestion.S3InternalConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component("ingestionValidator")
public class IngestionValidatorImpl implements IngestionValidator {
    private static final Logger log = LoggerFactory.getLogger(IngestionValidatorImpl.class);

    @Inject
    private IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @SuppressWarnings("unused")
    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @SuppressWarnings("unused")
    @Inject
    private S3Service s3Service;

    @Value(("${aws.region}"))
    private String defaultRegion;

    @Inject
    private ColumnMetadataProxy metadataProxy;

    @Override
    public boolean isIngestionTriggered(Ingestion ingestion) {
        if (!ingestion.isSchedularEnabled()) {
            return false;
        }
        // PATCH_BOOK ingestion is always manually triggered which is one of AM
        // rebuild operation
        if (ingestion.getIngestionType() == Ingestion.IngestionType.PATCH_BOOK) {
            return false;
        }
        if (StringUtils.isBlank(ingestion.getCronExpression()) || ingestion.getLatestTriggerTime() == null) {
            return true;
        }
        Date previousFireTime = CronUtils.getPreviousFireTime(ingestion.getCronExpression()).toDate();
        if (ingestion.getLatestTriggerTime().compareTo(previousFireTime) >= 0) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void validateIngestionRequest(Ingestion ingestion, IngestionRequest request) {
        if (StringUtils.isBlank(request.getSubmitter())) {
            throw new RuntimeException("Please provide submitter");
        }
        switch (ingestion.getIngestionType()) {
        case SFTP:
            if (StringUtils.isBlank(request.getFileName())) {
                throw new RuntimeException("Please provide file name");
            }
            SftpConfiguration sftpConfig = (SftpConfiguration) ingestion.getProviderConfiguration();
            if (!SftpUtils.ifFileExists(sftpConfig, request.getFileName())) {
                throw new IllegalArgumentException(String.format("File %s does not exist in SFTP % dir %s",
                        request.getFileName(), sftpConfig.getSftpHost(), sftpConfig.getSftpDir()));
            }
            return;
        case S3:
            if (StringUtils.isBlank(request.getFileName())) {
                throw new RuntimeException("Please provide file name");
            }
            S3Configuration s3Configuration = (S3Configuration) ingestion.getProviderConfiguration();
            if (Boolean.TRUE.equals(request.getUpdateCurrentVersion())) {
                s3Configuration.setUpdateCurrentVersion(true);
            }
            return;
        case BW_RAW:
            return;
        case PATCH_BOOK:
            if (StringUtils.isBlank(request.getSourceVersion())) {
                request.setSourceVersion(metadataProxy.latestVersion().getVersion());
            }
            PatchBookConfiguration patchConfig = (PatchBookConfiguration) ingestion.getProviderConfiguration();
            if (patchConfig.getBookType() == null || patchConfig.getPatchMode() == null) {
                throw new IllegalArgumentException(
                        "BookType or PatchMode is missing in PatchBookConfiguration. Please check Ingestion table in db");
            }
            // PatchBook validation takes place inside ingestion workflow due to
            // pagination is required and validation could take long time
            return;
        case S3_INTERNAL:
            if (StringUtils.isBlank(request.getFileName())) {
                throw new RuntimeException("Please provide file name");
            }
            S3InternalConfiguration config = (S3InternalConfiguration) ingestion.getProviderConfiguration();
            if (config.getSourceBucket() == null || config.getParentDir() == null
                    || config.getSubfolderDateFormat() == null) {
                throw new IllegalArgumentException(
                        "SourceBucket, ParentDir or SubfolderDateFormat is missing in S3InternalConfiguration. Please check Ingestion table in db");
            }
            return;
        default:
            throw new RuntimeException(
                    String.format("%s is not supported in ingest API", ingestion.getIngestionType()));
        }
    }

    @Override
    public boolean isDuplicateProgress(IngestionProgress progress) {
        return ingestionProgressEntityMgr.isDuplicateProgress(progress);
    }

    @Override
    public List<IngestionProgress> cleanupDuplicateProgresses(List<IngestionProgress> progresses) {
        Iterator<IngestionProgress> iter = progresses.iterator();
        while (iter.hasNext()) {
            IngestionProgress progress = iter.next();
            if (isDuplicateProgress(progress)) {
                iter.remove();
                log.info("Duplicate progress is ignored: " + progress.toString());
            }
        }
        return progresses;
    }
}
