package com.latticeengines.dellebi.service.impl;

import java.util.concurrent.Callable;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.dellebi.util.ExportAndReportService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public class DellEbiDailyJobCallable implements Callable<Boolean> {
    
    private static final Log log = LogFactory.getLog(DellEbiDailyJobCallable.class);
    
    private DailyFlow dailyFlow;
    private ExportAndReportService exportAndReportService;
    private String fileTypesList;
    
    public DellEbiDailyJobCallable(Builder builder) {
        this.dailyFlow = builder.getDailyFlow();
        this.exportAndReportService = builder.getExportAndReportService();
        this.fileTypesList = builder.getFileTypesList();
    }

    @Override
    public Boolean call() throws Exception {
        log.info("Start to process files from inbox.");
        long startTime = System.currentTimeMillis();

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
            log.error("EBI daily Job Flow did not find file or failed!");
        }

        long endTime = System.currentTimeMillis();
        log.info("Dell Ebi Job finished. Time elapsed="
                + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));
        return result;
    }
    
    public static class Builder {
        
        private DailyFlow dailyFlow;
        private ExportAndReportService exportAndReportService;
        private String fileTypesList;
        
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
        
        public Builder fileTypesList(String fileTypesList) {
            this.fileTypesList = fileTypesList;
            return this;
        }
        
        public DailyFlow getDailyFlow() {
            return dailyFlow;
        }
        
        public ExportAndReportService getExportAndReportService() {
            return exportAndReportService;
        }
        
        public String getFileTypesList() {
            return fileTypesList;
        }
    }
    

}
