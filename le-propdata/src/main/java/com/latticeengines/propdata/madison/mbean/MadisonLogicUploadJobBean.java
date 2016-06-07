package com.latticeengines.propdata.madison.mbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.madison.service.PropDataMadisonService;
import com.latticeengines.propdata.madison.service.impl.MadisonLogicUploadCallable;
import com.latticeengines.quartzclient.mbean.QuartzJobBean;

@Component("madisonLogicUpload")
public class MadisonLogicUploadJobBean implements QuartzJobBean {

    @Autowired
    private PropDataMadisonService propDataMadisonService;

    @Value("${propdata.jobs.enabled:false}")
    private boolean propdataJobsEnabled;

    @Override
    public Callable<Boolean> getCallable() {
        MadisonLogicUploadCallable.Builder builder = new MadisonLogicUploadCallable.Builder();
        builder.propdataJobsEnabled(propdataJobsEnabled)
                .propDataMadisonService(propDataMadisonService);
        return new MadisonLogicUploadCallable(builder);
    }

}
