package com.latticeengines.propdata.collection.entitymanager.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.dao.PivotProgressDao;
import com.latticeengines.propdata.collection.entitymanager.PivotProgressEntityMgr;
import com.latticeengines.propdata.collection.source.Source;

@Component("pivotProgressEntityMgr")
public class PivotProgressEntityMgrImpl
        extends AbstractProgressEntityMgr<PivotProgress> implements PivotProgressEntityMgr {

    @Autowired
    PivotProgressDao progressDao;

    @Override
    protected PivotProgressDao getProgressDao() { return progressDao; }

    @Override
    @Transactional(value = "propDataCollectionProgress")
    public PivotProgress insertNewProgress(Source source, Date pivotDate, String creator) {
        try {
            PivotProgress newProgress = PivotProgress.constructByDate(source.getSourceName(), pivotDate);
            newProgress.setCreatedBy(creator);
            progressDao.create(newProgress);
            return newProgress;
        } catch (IllegalAccessException|InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Transactional(value = "propDataCollectionProgress", readOnly = true)
    public PivotProgress findProgressNotInFinalState(Source source) {
        Set<ProgressStatus> finalStatus =
                new HashSet<>(Arrays.asList(ProgressStatus.UPLOADED, ProgressStatus.FAILED));
        List<PivotProgress> progresses = progressDao.findAllOfSource(source);
        for (PivotProgress progress: progresses) {
            if (!finalStatus.contains(progress.getStatus())) {
                return progress;
            }
        }
        return null;
    }

}
