package com.latticeengines.datacloud.etl.publication.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.service.DataCloudEngineVersionService;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;

@Component("publicationVersionService")
public class PublicationVersionServiceImpl implements DataCloudEngineVersionService {

    @Inject
    private PublicationEntityMgr publicationEntityMgr;

    @Inject
    private PublicationProgressEntityMgr progressEntityMgr;

    @Override
    public DataCloudEngineStage findProgressAtVersion(DataCloudEngineStage stage) {
        stage.setEngine(DataCloudEngine.PUBLICATION);
        Publication publication = publicationEntityMgr.findByPublicationName(stage.getEngineName());
        if (publication == null) {
            throw new RuntimeException("Publication with name : " + stage.getEngineName() + " does not exist");
        }
        List<PublicationProgress> progressStatus = progressEntityMgr.findStatusByPublicationVersion(publication,
                stage.getVersion());
        if (CollectionUtils.isEmpty(progressStatus)) {
            stage.setStatus(ProgressStatus.NOTSTARTED);
            stage.setProgress(null);
            stage.setMessage(null);
        } else {
            PublicationProgress progress = progressStatus.get(0);
            stage.setStatus(progress.getStatus());
            stage.setProgress(progress.getProgress());
            stage.setMessage(progress.getErrorMessage());
        }
        return stage;
    }

    @Override
    public DataCloudEngine getEngine() {
        return DataCloudEngine.PUBLICATION;
    }

    @Override
    public String findCurrentVersion(String publicationName) {
        return progressEntityMgr.getLatestSuccessVersion(publicationName);
    }
}
