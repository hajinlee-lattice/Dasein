package com.latticeengines.pls.qbean;

import java.util.concurrent.Callable;

import org.springframework.stereotype.Component;

import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("modelSummaryDownload")
public class ModelSummaryDownloadBean extends ModelSummaryDownloadAbstractBean implements QuartzJobBean {

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        super.setIncremental(true);
        return super.getCallable(jobArguments);
    }

}
