package com.latticeengines.datacloud.madison.jobs.impl;

import java.util.Date;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.madison.jobs.MadisonLogicJobService;
import com.latticeengines.datacloud.madison.service.PropDataContext;
import com.latticeengines.datacloud.madison.service.PropDataMadisonService;

@DisallowConcurrentExecution
@Component("madisonLogicUploadService")
public class MadisonLogicUploadServiceImpl extends QuartzJobBean implements MadisonLogicJobService {

    private static final Logger log = LoggerFactory.getLogger(MadisonLogicUploadServiceImpl.class);

    private PropDataMadisonService propDataMadisonService;

    private boolean propdataJobsEnabled = false;

    @Override
    protected void executeInternal(JobExecutionContext context) {

        long startTime = System.currentTimeMillis();

        try {
            log.info("Started!");
            if (!propdataJobsEnabled) {
                log.info("Job is disabled");
                return;
            }
            PropDataContext requestContextForTransformaton = new PropDataContext();
            PropDataContext responseContextForTransformaton = propDataMadisonService
                    .transform(requestContextForTransformaton);
            if (responseContextForTransformaton.getProperty(PropDataMadisonService.STATUS_KEY, String.class) == null
                    || !responseContextForTransformaton.getProperty(PropDataMadisonService.STATUS_KEY, String.class)
                            .equals(PropDataMadisonService.STATUS_OK)) {
                log.info("Finished! Upload job has no transformation.");
                return;
            }

            Date today = responseContextForTransformaton.getProperty(PropDataMadisonService.TODAY_KEY, Date.class);
            PropDataContext requestContextForUpload = new PropDataContext();
            requestContextForUpload.setProperty(PropDataMadisonService.TODAY_KEY, today);
            propDataMadisonService.exportToDB(requestContextForUpload);

            long endTime = System.currentTimeMillis();
            log.info("Finished! Eclipsed time="
                    + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));

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
