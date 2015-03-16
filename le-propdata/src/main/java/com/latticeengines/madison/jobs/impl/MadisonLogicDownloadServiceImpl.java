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
@Component("madisonLogicDownloadService")
public class MadisonLogicDownloadServiceImpl extends QuartzJobBean implements MadisonLogicJobService {

    private static final Log log = LogFactory.getLog(MadisonLogicDownloadServiceImpl.class);

    private PropDataMadisonService propMadisonDataService;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {

        long startTime = System.currentTimeMillis();
        try {
            PropDataContext requestContext = new PropDataContext();
            PropDataContext responseContext = propMadisonDataService.importFromDB(requestContext);

            long endTime = System.currentTimeMillis();
            log.info("Eclipsed time=" + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));
        } catch (Exception ex) {
            log.error("MadisonLogicDownloadService job failed!", ex);
        }
    }
}
