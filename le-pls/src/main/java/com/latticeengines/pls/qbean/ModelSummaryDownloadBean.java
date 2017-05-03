package com.latticeengines.pls.qbean;

import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.mbean.TimeStampContainer;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.impl.FeatureImportanceParser;
import com.latticeengines.pls.service.impl.ModelSummaryDownloadCallable;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("modelSummaryDownload")
public class ModelSummaryDownloadBean extends ModelSummaryDownloadAbstractBean implements QuartzJobBean {

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        super.setIncremental(true);
        return super.getCallable(jobArguments);
    }

}
