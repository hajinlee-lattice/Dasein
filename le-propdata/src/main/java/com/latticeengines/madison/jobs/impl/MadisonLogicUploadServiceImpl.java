package com.latticeengines.madison.jobs.impl;

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

    private PropDataMadisonService propMadisonDataService;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {

        long startTime = System.currentTimeMillis();

        try {
            PropDataContext requestContextForTransformaton = new PropDataContext();

            PropDataContext responseContextForTransformaton = propMadisonDataService
                    .transform(requestContextForTransformaton);

            PropDataContext requestContextForUpload = new PropDataContext();
            PropDataContext responseContextForUpload = propMadisonDataService.transform(requestContextForUpload);

            long endTime = System.currentTimeMillis();
            log.info("Eclipsed time=" + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));

        } catch (Exception ex) {
            log.error("MadisonLogicUploadServiceImpl job failed!", ex);
        }
    }

    public void setPropDataMadisonService(PropDataMadisonService propDataMadisonService) {
        this.propMadisonDataService = propDataMadisonService;
    }

}
