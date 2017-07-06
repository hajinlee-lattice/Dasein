package com.latticeengines.datacloud.madison.jobs.impl;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.madison.service.PropDataContext;
import com.latticeengines.datacloud.madison.service.PropDataMadisonService;

@DisallowConcurrentExecution
@Component("madisonLogicDownloadService")
public class MadisonLogicDownloadServiceImpl extends QuartzJobBean implements com.latticeengines.datacloud.madison.jobs.MadisonLogicJobService {

    private static final Log log = LogFactory.getLog(MadisonLogicDownloadServiceImpl.class);

    private PropDataMadisonService propDataMadisonService;
    private boolean propdataJobsEnabled = false;
    
    @SuppressWarnings("unused")
    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {

        long startTime = System.currentTimeMillis();
        try {
            log.info("Started!");
            if (!propdataJobsEnabled) {
                log.info("Job is disabled");
                return;
            }
            PropDataContext requestContext = new PropDataContext();
            PropDataContext responseContext = propDataMadisonService.importFromDB(requestContext);

            long endTime = System.currentTimeMillis();
            log.info("Finished! Eclipsed time=" + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));
            
        } catch (Exception ex) {
            log.error("Failed!", ex);
        }
    }

    public void setPropDataMadisonService(PropDataMadisonService propDataMadisonService) {
        this.propDataMadisonService = propDataMadisonService;
    }

    public void setPropdataJobsEnabled(boolean propdataJobsEnabled) {
        this.propdataJobsEnabled = propdataJobsEnabled;
    }
    
    
}
