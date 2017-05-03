package com.latticeengines.pls.qbean;

import java.util.concurrent.Callable;

import org.springframework.stereotype.Component;

import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("modelSummaryFullDownload")
public class ModelSummaryFullDownloadBean extends ModelSummaryDownloadAbstractBean implements QuartzJobBean {
    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        super.setIncremental(false);
        return super.getCallable(jobArguments);
    }
}
