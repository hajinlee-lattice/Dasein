package com.latticeengines.dellebi.dataprocess;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.util.SqoopDataService;

public class DailyJob  extends QuartzJobBean  {
	
	private DailyFlow dailyFlow;
	private SqoopDataService sqoopDataService;

    private static final Log log = LogFactory.getLog(DailyJob.class);

    private void process() {

    	log.info("Start to process files from inbox.");      
        
        // Process data using Cascading 
        // Note Camel scans input folder intermittently so it archive and unzip data continuously if there's new data incomes.
        // But Cascading does it's job once when it be called.  So there's only time Casadading processes data and new data incomes after that,
        // Cascading processes it next day.
    	dailyFlow.doDailyFlow();
    	
    	sqoopDataService.export();
            	  	
        //Send notifications to inform Daily refresh is done.
        log.info("EBI daily refresh just finished successfully.");
    }

    @Override
    public void executeInternal(JobExecutionContext context) throws JobExecutionException {

        try {
            process();
        } catch (Exception e) {
        	log.error("Failed to execute daily job!", e);
        }

    }
    
    public void setDailyFlow(DailyFlow dailyFlow){
        this.dailyFlow = dailyFlow;
    }
    
    public void setSqoopDataService(SqoopDataService sqoopDataService){
        this.sqoopDataService = sqoopDataService;
    }
}
