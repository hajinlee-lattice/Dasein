package com.latticeengines.madison.jobs.impl;


import java.util.Date;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.madison.jobs.MadisonLogicJobService;
import com.latticeengines.madison.service.PropDataMadisonService;
import com.latticeengines.propdata.service.db.PropDataContext;

@DisallowConcurrentExecution
@Component("madisonLogicUploadService")
public class MadisonLogicUploadServiceImpl extends QuartzJobBean implements MadisonLogicJobService {

    private static final Log log = LogFactory.getLog(MadisonLogicUploadServiceImpl.class);

    private PropDataMadisonService propDataMadisonService;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {

        long startTime = System.currentTimeMillis();

        try {
            log.info("Started!");
            PropDataContext requestContextForTransformaton = new PropDataContext();
            PropDataContext responseContextForTransformaton = propDataMadisonService
                    .transform(requestContextForTransformaton);
            if (!responseContextForTransformaton.getProperty(PropDataMadisonService.STATUS_KEY, String.class).equals(
                    PropDataMadisonService.STATUS_OK)) {
                log.info("Finished! Upload job has no transformation.");
                return;
            }

            Date today = responseContextForTransformaton.getProperty(PropDataMadisonService.TODAY_KEY, Date.class);
            PropDataContext requestContextForUpload = new PropDataContext();
            requestContextForUpload.setProperty(PropDataMadisonService.TODAY_KEY, today);
            PropDataContext responseContextForUpload = propDataMadisonService.exportToDB(requestContextForUpload);

            long endTime = System.currentTimeMillis();
            log.info("Finished! Eclipsed time=" + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));

        } catch (Exception ex) {
            log.error("Failed!", ex);
        }
    }

    public void setPropDataMadisonService(PropDataMadisonService propDataMadisonService) {
        this.propDataMadisonService = propDataMadisonService;
    }
    
    

}
