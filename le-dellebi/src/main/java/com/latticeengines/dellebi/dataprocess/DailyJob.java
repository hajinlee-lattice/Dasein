package com.latticeengines.dellebi.dataprocess;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.dellebi.flowdef.DailyFlow;
import com.latticeengines.dellebi.util.MailSender;

public class DailyJob implements Job {

    public final static Logger LOGGER = Logger.getLogger(DailyJob.class);

    private void process() {

        // Get files and unzip them.
        LOGGER.info("Start to process files from inbox.");

        // Apache Camel starts to process data when Spring ApplicationContext starts.
        // Apache Camel will not stop once it starts.
        ApplicationContext springContext = new ClassPathXmlApplicationContext("dellebi-properties-context.xml",
                "dellebi-context.xml");
        
        //Wait for a while to let Camel process data.
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            LOGGER.info("Thread sleep has be interrupted. ", e);
        }
        
        // Process data using Cascading 
        // Note Camel scans input folder intermittently so it archive and unzip data continuously if there's new data incomes.
        // But Cascading does it's job once when it be called.  So there's only time Casadading processes data and new data incomes after that,
        // Cascading processes it next day.
        DailyFlow dailFlow = springContext.getBean("dailyFlow", DailyFlow.class);
        dailFlow.doDailyFlow(springContext);
        
        //Send notifications to inform Daily refresh is done.
        LOGGER.info("EBI daily refresh just finished successfully.");
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        try {
            process();
        } catch (Exception e) {
            LOGGER.error("Failed to execute daily job!", e);
        }

    }
}
