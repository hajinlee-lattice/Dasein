package com.latticeengines.release.jenkins.activities;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.latticeengines.release.error.handler.ErrorHandler;

@Configuration
public class RunJenkinsDeploymentJobActivityConfiguration {

    @Bean(name = "dpDeploymentJobActivity")
    public RunJenkinsDeploymentJobActivity runDPDeploymentJobActivity(
            @Value("${release.jenkins.deployment.dp}") String url,
            @Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        return new RunJenkinsDeploymentJobActivity(url, errorHandler);
    }

    @Bean(name = "plsDeploymentJobActivity")
    public RunJenkinsDeploymentJobActivity runPlsDeploymentJobActivity(
            @Value("${release.jenkins.deployment.pls}") String url,
            @Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        return new RunJenkinsDeploymentJobActivity(url, errorHandler);
    }
}
