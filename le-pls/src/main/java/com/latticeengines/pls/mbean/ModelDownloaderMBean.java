package com.latticeengines.pls.mbean;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.net.ntp.TimeStamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

@Component("modelDownloaderMBean")
@ManagedResource(objectName = "Diagnostics:name=ModelDownloaderCheck")
public class ModelDownloaderMBean{

	@Autowired
    private ApplicationContext applicationContext;

    @Value("${pls.jmx.downloader.check.frequency}")
    private long downloaderInvokeInterval;

    @Autowired
    private TimeStampContainer timeStampContainer;

	@ManagedOperation(description = "Check if ModelDownloader is Running")
    public String checkModelDownloader() {
        try {
            if (applicationContext.containsBean("modelSummaryDownloadJob")){
                TimeStamp timeStamp = TimeStamp.getCurrentTime();
                if (timeStamp.getSeconds() - timeStampContainer.getTimeStamp().getSeconds() <= downloaderInvokeInterval){
                    long diff = timeStamp.getSeconds() - timeStampContainer.getTimeStamp().getSeconds();
                    return String.format("[SUCCESS] ModelDownloader job has been running for %d seconds.", diff);
                }
                return String.format("[FAILURE] ModelDownloaderJob has been running for more than %d seconds.", downloaderInvokeInterval);
            }
            return "[FAILURE] ModelDownloaderJob is not loaded into application context.";
        } catch (Exception e) {
            return "[FAILURE] Unexpected exception happened: \n"  + ExceptionUtils.getStackTrace(e);
        }
    }
}
