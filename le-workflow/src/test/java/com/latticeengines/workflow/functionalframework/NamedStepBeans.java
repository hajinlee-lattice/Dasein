package com.latticeengines.workflow.functionalframework;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NamedStepBeans {

    @Bean
    public NamedStep stepA() {
        return new NamedStep("A");
    }

    @Bean
    public NamedStep stepB() {
        return new NamedStep("B");
    }

    @Bean
    public NamedStep stepC() {
        return new NamedStep("C");
    }

    @Bean
    public NamedStep stepD() {
        return new NamedStep("D");
    }

    @Bean
    public NamedStep stepSkippedOnMissingConfig() {
        return new NamedStep("SkippedOnMissingConfig", true);
    }

}
