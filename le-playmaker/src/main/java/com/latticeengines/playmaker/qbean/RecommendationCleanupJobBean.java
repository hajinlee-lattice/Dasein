package com.latticeengines.playmaker.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.playmaker.service.RecommendationCleanupService;
import com.latticeengines.playmaker.service.impl.RecommendationCleanupCallable;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("recommendationCleanupJob")
public class RecommendationCleanupJobBean implements QuartzJobBean {

    private static final Logger log = LoggerFactory.getLogger(RecommendationCleanupJobBean.class);

    @Inject
    private RecommendationCleanupService recommendationCleanupService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        log.info(String.format("Got callback with job arguments = %s", jobArguments));

        RecommendationCleanupCallable.Builder builder = new RecommendationCleanupCallable.Builder();
        builder.recommendationCleanupService(recommendationCleanupService);
        return new RecommendationCleanupCallable(builder);
    }

}
