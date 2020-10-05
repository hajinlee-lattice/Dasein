package com.latticeengines.matchapi.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.VboUsageService;
import com.latticeengines.matchapi.service.impl.SubmitVboUsageReportCallable;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("submitVboUsageReportJob")
public class SubmitVboUsageReportJobBean implements QuartzJobBean {

    @Inject
    private VboUsageService vboUsageService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        SubmitVboUsageReportCallable.Builder builder = new SubmitVboUsageReportCallable.Builder();
        builder.vboUsageService(vboUsageService);
        return new SubmitVboUsageReportCallable(builder);
    }

}
