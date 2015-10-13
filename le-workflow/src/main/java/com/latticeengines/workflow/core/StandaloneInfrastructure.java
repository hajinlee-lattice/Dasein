package com.latticeengines.workflow.core;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing(modular=true)
public class StandaloneInfrastructure {
}
