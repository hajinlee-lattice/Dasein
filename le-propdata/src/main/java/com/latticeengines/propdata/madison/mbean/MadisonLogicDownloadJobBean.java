package com.latticeengines.propdata.madison.mbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.madison.service.PropDataMadisonService;
import com.latticeengines.propdata.madison.service.impl.MadisonLogicDownloadCallable;
import com.latticeengines.quartzclient.mbean.QuartzJobBean;

@Component("madisonLogicDownload")
public class MadisonLogicDownloadJobBean implements QuartzJobBean {

    @Autowired
    private PropDataMadisonService propDataMadisonService;

    @Value("${propdata.jobs.enabled:false}")
    private boolean propdataJobsEnabled;

    @Override
    public Callable<Boolean> getCallable() {

        MadisonLogicDownloadCallable.Builder builder = new MadisonLogicDownloadCallable.Builder();
        builder.propDataMadisonService(propDataMadisonService)
                .propdataJobsEnabled(propdataJobsEnabled);
        return new MadisonLogicDownloadCallable(builder);
    }

}
