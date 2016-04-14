package com.latticeengines.propdata.collection.entitymgr.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublicationDestination;
import com.latticeengines.propdata.collection.dao.PublicationDao;
import com.latticeengines.propdata.collection.dao.PublicationProgressDao;
import com.latticeengines.propdata.collection.entitymgr.PublicationProgressEntityMgr;

@Component("publicationProgressEntityMgr")
public class PublicationProgressEntityMgrImpl implements PublicationProgressEntityMgr {

    @Autowired
    private PublicationProgressDao progressDao;

    @Autowired
    private PublicationDao publicationDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public PublicationProgress findBySourceVersionUnderMaximumRetry(Publication publication, String sourceVersion) {
        Publication publication1 = publicationDao.findByField("PublicationName", publication.getPublicationName());
        List<PublicationProgress> progressList = publication1.getProgresses();
        for (PublicationProgress progress : progressList) {
            if (sourceVersion.equals(progress.getSourceVersion()) && !canBeIgnored(publication, progress)) {
                return progress;
            }
        }
        return null;
    }

    @Override
    @Transactional(value = "propDataManage")
    public PublicationProgress startNewProgress(Publication publication, PublicationDestination destination,
            String sourceVersion, String creator) {
        PublicationProgress progress = new PublicationProgress();
        progress.setPublication(publication);
        progress.setSourceVersion(sourceVersion);
        progress.setCreatedBy(creator);
        progress.setDestination(destination);

        progress.setCreateTime(new Date());
        progress.setLatestStatusUpdate(new Date());
        progress.setProgress(0f);
        progress.setRetries(0);
        progress.setStatus(PublicationProgress.Status.NEW);
        progressDao.create(progress);

        return findBySourceVersionUnderMaximumRetry(publication, sourceVersion);
    }

    @Override
    @Transactional(value = "propDataManage")
    public PublicationProgress updateProgress(PublicationProgress progress) {
        progressDao.update(progress);
        return progressDao.findByKey(progress);
    }

    private Boolean canBeIgnored(Publication publication, PublicationProgress progress) {
        return (progress.getRetries() >= publication.getNewJobMaxRetry()
                && PublicationProgress.Status.FAILED.equals(progress.getStatus()));
    }

}
