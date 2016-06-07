package com.latticeengines.quartz.service;

import java.util.concurrent.Callable;

import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import com.latticeengines.quartzclient.mbean.QuartzJobBean;

@Configuration
@Component("testPredefinedJob")
public class TestPredefinedJobBean implements QuartzJobBean {

    private String msg = "++++++++++++++++ try print msg: predefined job! ++++++++++++++++";

    @Override
    public Callable<Boolean> getCallable() {
        TestPredefinedJobCallable.Builder builder = new TestPredefinedJobCallable.Builder();
        builder.outputMsg(msg);
        return new TestPredefinedJobCallable(builder);
    }

}
