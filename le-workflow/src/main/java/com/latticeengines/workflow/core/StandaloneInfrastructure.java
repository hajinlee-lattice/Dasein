package com.latticeengines.workflow.core;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.context.annotation.Import;

//@Configuration
@EnableBatchProcessing(modular=true)
@Import(JobOperatorInfrastructure.class)
public class StandaloneInfrastructure {
}
