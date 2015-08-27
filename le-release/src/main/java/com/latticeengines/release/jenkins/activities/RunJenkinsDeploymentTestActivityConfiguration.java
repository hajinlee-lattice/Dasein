package com.latticeengines.release.jenkins.activities;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.latticeengines.release.error.handler.ErrorHandler;

@Configuration
public class RunJenkinsDeploymentTestActivityConfiguration {

    @Bean(name = "dpDeploymentTestActivity")
    public RunJenkinsDeploymentTestActivity getDataplatformDeploymentTestActivity(
            @Value("${release.jenkins.dp.test.deployment.url}") String url,
            @Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        return new RunJenkinsDeploymentTestActivity(url, errorHandler);
    }
    
    @Bean(name = "orcDeploymentTestActivity")
    public RunJenkinsDeploymentTestActivity getOrchestrationDeploymentTestActivity(
            @Value("${release.jenkins.orc.test.deployment.url}") String url,
            @Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        return new RunJenkinsDeploymentTestActivity(url, errorHandler);
    }

    @Bean(name = "plsDeploymentTestActivity")
    public RunJenkinsDeploymentTestActivity getPlsDeploymentTestActivity(
            @Value("${release.jenkins.pls.test.deployment.url}") String url,
            @Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        return new RunJenkinsDeploymentTestActivity(url, errorHandler);
    }

    @Bean(name = "plsProtractorTestActivity")
    public RunJenkinsDeploymentTestActivity getPlsProtractorTestActivity(
            @Value("${release.jenkins.pls.test.protractor.url}") String url,
            @Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        return new RunJenkinsDeploymentTestActivity(url, errorHandler);
    }

    @Bean(name = "scoringDeploymentTestActivity")
    public RunJenkinsDeploymentTestActivity getScoringDeploymentTestActivity(
            @Value("${release.jenkins.scoring.test.deployment.url}") String url,
            @Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        return new RunJenkinsDeploymentTestActivity(url, errorHandler);
    }

}
