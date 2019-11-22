package com.latticeengines.datacloud.etl.publication.entitymgr.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.publication.dao.PublicationDao;
import com.latticeengines.datacloud.etl.publication.dao.PublicationProgressDao;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationDestination;

@Component("publicationProgressEntityMgr")
public class PublicationProgressEntityMgrImpl implements PublicationProgressEntityMgr {

    @Inject
    private PublicationProgressDao progressDao;

    @Inject
    private PublicationDao publicationDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public PublicationProgress findBySourceVersionUnderMaximumRetry(@NotNull Publication publication,
            @NotNull String sourceVersion) {
        Preconditions.checkNotNull(publication);
        Preconditions.checkNotNull(publication.getPublicationName());
        Preconditions.checkNotNull(sourceVersion);
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
    public PublicationProgress startNewProgress(@NotNull Publication publication,
            @NotNull PublicationDestination destination, @NotNull String sourceVersion, @NotNull String creator) {
        Preconditions.checkNotNull(publication);
        Preconditions.checkNotNull(destination);
        Preconditions.checkNotNull(sourceVersion);
        Preconditions.checkNotNull(creator);

        PublicationProgress progress = newProgress(publication, destination, sourceVersion, creator);
        progress.setStatus(ProgressStatus.NEW);
        progressDao.create(progress);
        return findBySourceVersionUnderMaximumRetry(publication, sourceVersion);
    }

    @Override
    @Transactional(value = "propDataManage")
    public PublicationProgress runNewProgress(@NotNull Publication publication,
            @NotNull PublicationDestination destination, @NotNull String sourceVersion, @NotNull String creator) {
        Preconditions.checkNotNull(publication);
        Preconditions.checkNotNull(destination);
        Preconditions.checkNotNull(sourceVersion);
        Preconditions.checkNotNull(creator);

        PublicationProgress progress = newProgress(publication, destination, sourceVersion, creator);
        progress.setStatus(ProgressStatus.PROCESSING);
        progressDao.create(progress);
        return findBySourceVersionUnderMaximumRetry(publication, sourceVersion);
    }

    @Override
    @Transactional(value = "propDataManage")
    public PublicationProgress updateProgress(@NotNull PublicationProgress progress) {
        Preconditions.checkNotNull(progress);

        progressDao.update(progress);
        return progressDao.findByKey(progress);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public PublicationProgress findLatestNonTerminalProgress(@NotNull Publication publication) {
        Preconditions.checkNotNull(publication);
        Preconditions.checkNotNull(publication.getPublicationName());

        Publication publication1 = publicationDao.findByField("PublicationName", publication.getPublicationName());
        List<PublicationProgress> progressList = publication1.getProgresses();
        Collections.sort(progressList, Comparator.comparing(PublicationProgress::getCreateTime));
        for (PublicationProgress progress : progressList) {
            if (canProceed(publication, progress)) {
                return progress;
            }
        }
        return null;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public PublicationProgress findLatestUnderMaximumRetry(@NotNull Publication publication) {
        Preconditions.checkNotNull(publication);
        Preconditions.checkNotNull(publication.getPublicationName());

        Publication publication1 = publicationDao.findByField("PublicationName", publication.getPublicationName());
        List<PublicationProgress> progressList = publication1.getProgresses();
        Collections.sort(progressList, Comparator.comparing(PublicationProgress::getCreateTime));
        for (PublicationProgress progress : progressList) {
            if (!canBeIgnored(publication, progress)) {
                return progress;
            }
        }
        return null;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<PublicationProgress> findAllForPublication(@NotNull Publication publication) {
        Preconditions.checkNotNull(publication);
        Preconditions.checkArgument(publication.getPid() != null || publication.getPublicationName() != null);

        if (publication.getPid() == null) {
            publication = publicationDao.findByField("PublicationName", publication.getPublicationName());
        }
        return progressDao.findAllForPublication(publication.getPid());
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public PublicationProgress findByPid(@NotNull Long pid) {
        Preconditions.checkNotNull(pid);

        return progressDao.findByKey(PublicationProgress.class, pid);
    }

    @Override
    @Transactional(value = "propDataManage")
    public List<PublicationProgress> findStatusByPublicationVersion(@NotNull Publication publication,
            @NotNull String version) {
        Preconditions.checkNotNull(publication);
        Preconditions.checkNotNull(version);

        return progressDao.getStatusForLatestVersion(publication, version);
    }


    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public String getLatestSuccessVersion(@NotNull String publicationName) {
        Preconditions.checkNotNull(publicationName);

        return progressDao.getLatestSuccessVersion(publicationName);
    }

    private Boolean canBeIgnored(Publication publication, PublicationProgress progress) {
        return (progress.getRetries() >= publication.getNewJobMaxRetry()
                && ProgressStatus.FAILED.equals(progress.getStatus()));
    }

    private Boolean canProceed(Publication publication, PublicationProgress progress) {
        if (progress.getRetries() == null) {
            progress.setRetries(0);
        }
        return ProgressStatus.NEW.equals(progress.getStatus()) || (ProgressStatus.FAILED.equals(progress.getStatus())
                && progress.getRetries() < publication.getNewJobMaxRetry());
    }

    private PublicationProgress newProgress(Publication publication, PublicationDestination destination,
                                            String sourceVersion, String creator) {
        PublicationProgress progress = new PublicationProgress();
        progress.setPublication(publication);
        progress.setSourceVersion(sourceVersion);
        progress.setCreatedBy(creator);
        progress.setDestination(destination);
        progress.setHdfsPod(HdfsPodContext.getHdfsPodId());

        progress.setCreateTime(new Date());
        progress.setLatestStatusUpdate(new Date());
        progress.setProgress(0f);
        progress.setRetries(0);
        progress.setStatus(ProgressStatus.NEW);
        return progress;
    }
}
