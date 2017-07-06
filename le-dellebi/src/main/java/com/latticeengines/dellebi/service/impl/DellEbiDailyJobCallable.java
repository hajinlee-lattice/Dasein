package com.latticeengines.dellebi.service.impl;

import java.util.concurrent.Callable;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.newrelic.api.agent.Trace;

public class DellEbiDailyJobCallable implements Callable<Boolean> {

    private static final Log log = LogFactory.getLog(DellEbiDailyJobCallable.class);
    private final String quartzJob;

    private DailyFlow dailyFlow;
    private ExportAndReportService exportAndReportService;
    private DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    public DellEbiDailyJobCallable(Builder builder) {

        this.dailyFlow = builder.getDailyFlow();
        this.exportAndReportService = builder.getExportAndReportService();
        this.dellEbiConfigEntityMgr = builder.dellEbiConfigEntityMgr;
        this.quartzJob = builder.quartzJob;
    }

    @Override
    @Trace(dispatcher = true)
    public Boolean call() throws Exception {
        log.info("Start to process files from inbox.");
        long startTime = System.currentTimeMillis();

        dellEbiConfigEntityMgr.initialService();
        String fileTypesList = dellEbiConfigEntityMgr.getFileTypesByQuartzJob(quartzJob);
        DataFlowContext context = dailyFlow.doDailyFlow(fileTypesList.split(","));
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
        	boolean noFileFound = context.getProperty(DellEbiFlowService.NO_FILE_FOUND, Boolean.class);
        	if (noFileFound) {
                log.info("EBI daily Job Flow didn't find any file to process.");        		        		
        	} else {
                log.info("EBI daily Job Flow failed!");        		
        	}
        }

        long endTime = System.currentTimeMillis();
        log.info("Dell Ebi Job finished. Time elapsed="
                + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));
        return result;
    }

    public static class Builder {

        private DailyFlow dailyFlow;
        private ExportAndReportService exportAndReportService;
        private DellEbiConfigEntityMgr dellEbiConfigEntityMgr;
        private String quartzJob;

        public Builder() {

        }

        public Builder dailyFlow(DailyFlow dailyFlow) {
            this.dailyFlow = dailyFlow;
            return this;
        }

        public Builder exportAndReportService(ExportAndReportService exportAndReportService) {
            this.exportAndReportService = exportAndReportService;
            return this;
        }

        public Builder dellEbiConfigEntityMgr(DellEbiConfigEntityMgr dellEbiConfigEntityMgr) {
            this.dellEbiConfigEntityMgr = dellEbiConfigEntityMgr;
            return this;
        }

        public Builder quartzJob(String quartzJob) {
            this.quartzJob = quartzJob;
            return this;
        }

        public DailyFlow getDailyFlow() {
            return dailyFlow;
        }

        public ExportAndReportService getExportAndReportService() {
            return exportAndReportService;
        }

        public DellEbiConfigEntityMgr getDellEbiConfigEntityMgr() {
            return dellEbiConfigEntityMgr;
        }
    }

}
