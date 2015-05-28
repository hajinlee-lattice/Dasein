package com.latticeengines.dellebi.dataprocess;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@DisallowConcurrentExecution
public class DellEbiDailyJob extends QuartzJobBean {

    private DailyFlow dailyFlow;
    private ExportAndReportService exportAndReportService;

    private static final Log log = LogFactory.getLog(DellEbiDailyJob.class);

    private void process() {

        log.info("Start to process files from inbox.");
        long startTime = System.currentTimeMillis();

        DataFlowContext context = dailyFlow.doDailyFlow();
        boolean result = context.getProperty(DellEbiFlowService.RESULT_KEY, Boolean.class);
        if (result) {
            log.info("EBI daily Job Flow finished successfully.");
            context.setProperty(DellEbiFlowService.START_TIME, startTime);
            result = exportAndReportService.export(context);
            if (result) {
                log.info("EBI daily refresh (export) finished successfully.");
            } else {
                log.info("EBI daily refresh (export) failed.");
            }

        } else {
            log.error("EBI daily Job Flow did not find file or failed!");
        }

        long endTime = System.currentTimeMillis();
        log.info("Dell Ebi Job finished. Time elapsed="
                + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));

    }

    @Override
    public void executeInternal(JobExecutionContext context) throws JobExecutionException {

        try {
            process();
        } catch (Exception e) {
            log.error("Failed to execute daily job!", e);
        }
    }

    public void setDailyFlow(DailyFlow dailyFlow) {
        this.dailyFlow = dailyFlow;
    }

    public void setExportAndReportService(ExportAndReportService exportAndReportService) {
        this.exportAndReportService = exportAndReportService;
    }
}
