package com.latticeengines.propdata.collection.entitymgr.impl;

import java.util.Collections;
import java.util.Comparator;
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

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public PublicationProgress findLatestNonTerminalProgress(Publication publication) {
        Publication publication1 = publicationDao.findByField("PublicationName", publication.getPublicationName());
        List<PublicationProgress> progressList = publication1.getProgresses();
        Collections.sort(progressList, new Comparator<PublicationProgress>() {
            @Override
            public int compare(PublicationProgress o1, PublicationProgress o2) {
                // reverse in create time
                return o2.getCreateTime().compareTo(o1.getCreateTime());
            }
        });
        for (PublicationProgress progress: progressList) {
            if (canProceed(publication, progress)) {
                return progress;
            }
        }
        return null;
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<PublicationProgress> findAllForPublication(Publication publication) {
        if (publication.getPid() == null) {
            publication = publicationDao.findByField("PublicationName", publication.getPublicationName());
        }
        return progressDao.findAllForPublication(publication.getPid());
    }

    private Boolean canBeIgnored(Publication publication, PublicationProgress progress) {
        return (progress.getRetries() >= publication.getNewJobMaxRetry()
                && PublicationProgress.Status.FAILED.equals(progress.getStatus()));
    }

    private Boolean canProceed(Publication publication, PublicationProgress progress) {
        if (progress.getRetries() == null) {
            progress.setRetries(0);
        }
        return PublicationProgress.Status.NEW.equals(progress.getStatus())
                || (PublicationProgress.Status.FAILED.equals(progress.getStatus())
                        && progress.getRetries() < publication.getNewJobMaxRetry());
    }

}
