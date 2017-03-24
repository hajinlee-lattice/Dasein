package com.latticeengines.datacloud.madison.mbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.madison.service.PropDataMadisonService;
import com.latticeengines.datacloud.madison.service.impl.MadisonLogicDownloadCallable;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("madisonLogicDownload")
public class MadisonLogicDownloadJobBean implements QuartzJobBean {

    @Autowired
    private PropDataMadisonService propDataMadisonService;

    @Value("${propdata.jobs.enabled:false}")
    private boolean propdataJobsEnabled;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {

        MadisonLogicDownloadCallable.Builder builder = new MadisonLogicDownloadCallable.Builder();
        builder.propDataMadisonService(propDataMadisonService)
                .propdataJobsEnabled(propdataJobsEnabled);
        return new MadisonLogicDownloadCallable(builder);
    }

}
