package com.latticeengines.quartz.service;

import java.util.concurrent.Callable;

import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Configuration
@Component("testQuartzJob")
public class TestQuartzJobBean implements QuartzJobBean {

    private String msg = "++++++++++++++++ try print msg: test quartz job! ++++++++++++++++";

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        TestQuartzJobCallable.Builder builder = new TestQuartzJobCallable.Builder();
        builder.outputMsg(msg).jobArgument(jobArguments);
        return new TestQuartzJobCallable(builder);
    }

}
