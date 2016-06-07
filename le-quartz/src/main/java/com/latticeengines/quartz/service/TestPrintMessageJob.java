package com.latticeengines.quartz.service;

import java.util.Date;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class TestPrintMessageJob extends QuartzJobBean {
	
	public static final String MESSAGE = "message";
	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		try{
			JobDataMap data = context.getJobDetail().getJobDataMap();
	        String message = data.getString(MESSAGE);
	        System.out.println(message + new Date());
	        
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
