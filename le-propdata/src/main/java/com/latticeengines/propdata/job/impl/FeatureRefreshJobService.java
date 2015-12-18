package com.latticeengines.propdata.job.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.CollectionSource;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.job.RefreshJobService;

@DisallowConcurrentExecution
@Component("featureRefreshJobService")
public class FeatureRefreshJobService extends AbstractCollectionSourceRefreshJobService implements RefreshJobService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    @Qualifier(value = "featureArchiveService")
    private ArchiveService archiveService;

    @Autowired
    @Qualifier(value = "featurePivotService")
    private PivotService pivotService;

    @Autowired
    @Qualifier(value = "archiveProgressEntityMgr")
    private ArchiveProgressEntityMgr archiveProgressEntityMgr;

    private boolean quartzEnabled = false;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        if (quartzEnabled) {
            try {
                super.executeInternal(context);
            } catch (Exception e) {
                log.error("Archiving " + getSource().getSourceName() + "Failed.", e);
                throw new JobExecutionException(e);
            }
        }
    }

    @Override
    ArchiveService getArchiveService() { return archiveService; }

    @Override
    ArchiveProgressEntityMgr getArchiveProgressEntityMgr() { return archiveProgressEntityMgr; }

    @Override
    Log getLog() { return log; }

    @Override
    CollectionSource getSource() { return CollectionSource.FEATURE; }

    @Override
    protected void proceedProgress(ArchiveProgress progress) {
        super.proceedProgress(progress);

        // generate pivoted table
        pivotData(progress.getEndDate(), hdfsSourceEntityMgr.getCurrentVersion(getSource()));
    }

    @Override
    public void pivotData(Date pivotDate, String featureSourceVersion) {
        PivotedSource pivotedSource = PivotedSource.FEATURE_PIVOTED;
        try {
            PivotProgress pivotProgress = pivotService.startNewProgress(pivotDate, featureSourceVersion, jobSubmitter);
            pivotProgress = pivotService.pivot(pivotProgress);
            pivotProgress = pivotService.exportToDB(pivotProgress);
            log.info(String.format("Pivoting %s successful, generated Rows=%d", pivotedSource.getSourceName(),
                    pivotProgress.getRowsGenerated()));
        } catch (Exception e) {
            log.fatal("Failed to pivot " + pivotedSource.getSourceName());
        }
    }

    // set job data as map
    @SuppressWarnings("unused")
    public void setArchiveService(ArchiveService archiveService) {
        this.archiveService = archiveService;
    }

    @SuppressWarnings("unused")
    public void setPivotService(PivotService pivotService) {
        this.pivotService = pivotService;
    }

    @SuppressWarnings("unused")
    public void setArchiveProgressEntityMgr(ArchiveProgressEntityMgr archiveProgressEntityMgr) {
        this.archiveProgressEntityMgr = archiveProgressEntityMgr;
    }

    @SuppressWarnings("unused")
    public void setQuartzEnabled(boolean quartzEnabled) { this.quartzEnabled = quartzEnabled; }

}
