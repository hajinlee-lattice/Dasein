package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.text.ParseException;
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
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionValidator;
import com.latticeengines.datacloud.etl.utils.SftpUtils;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.ingestion.PatchBookConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.S3Configuration;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.SqlToSourceConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.SqlToSourceConfiguration.CollectCriteria;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.PatchProxy;

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

    @Inject
    private PatchProxy patchProxy;

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
        if (StringUtils.isNotBlank(ingestion.getCronExpression())) {
            return ingestionProgressEntityMgr.isIngestionTriggered(ingestion);
        }
        return true;
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
        case SQL_TO_SOURCE:
            if (StringUtils.isBlank(request.getSourceVersion())) {
                request.setSourceVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            }
            try {
                HdfsPathBuilder.dateFormat.parse(request.getSourceVersion());
            } catch (ParseException e) {
                throw new IllegalArgumentException(String.format(
                        "Version %s is not in valid format. eg. 2017-01-01_00-00-00_UTC", request.getSourceVersion()));
            }
            SqlToSourceConfiguration sqlToSourceConfig = (SqlToSourceConfiguration) ingestion
                    .getProviderConfiguration();
            if (sqlToSourceConfig.getCollectCriteria() == CollectCriteria.NEW_DATA) {
                String currentVersion = hdfsSourceEntityMgr.getCurrentVersion(sqlToSourceConfig.getSource());
                if (currentVersion != null && currentVersion.compareTo(request.getSourceVersion()) >= 0) {
                    throw new IllegalArgumentException(
                            "For collect criteria NEW_DATA, target source version cannot be earlier than current version");
                }
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
