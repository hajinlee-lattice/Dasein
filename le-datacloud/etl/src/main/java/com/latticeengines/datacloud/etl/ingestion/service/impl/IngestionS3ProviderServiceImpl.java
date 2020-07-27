package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.yarn.LedpQueueAssigner;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.S3SourceEntityMgr;
import com.latticeengines.domain.exposed.datacloud.ingestion.S3Destination;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Service("ingestionS3Provider")
public class IngestionS3ProviderServiceImpl extends IngestionProviderServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(IngestionS3ProviderServiceImpl.class);

    @Inject
    private S3SourceEntityMgr s3SourceEntityMgr;

    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    private EMREnvService emrEnvService;

    @Override
    public void ingest(IngestionProgress progress) {
        String destStr = progress.getDestination();
        S3Destination destination = JsonUtils.deserialize(destStr, S3Destination.class);
        log.info("Start ingesting to: " + JsonUtils.serialize(destination));

        String sourceName = destination.getSourceName();
        String sourceVersion = destination.getSourceVersion();
        if (StringUtils.isBlank(sourceName) || StringUtils.isBlank(sourceVersion)) {
            throw new IllegalArgumentException("Must provide both source name and source version");
        }
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        queue = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
        s3SourceEntityMgr.downloadToHdfs(sourceName, sourceVersion, queue);

        if (Boolean.TRUE.equals(destination.getUpdateCurrentVersion())) {
            log.info("Updating __CURRENT_VERSION to " + sourceVersion);
            hdfsSourceEntityMgr.setCurrentVersion(sourceName, sourceVersion);
        }
    }

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        return Collections.emptyList();
    }

}
