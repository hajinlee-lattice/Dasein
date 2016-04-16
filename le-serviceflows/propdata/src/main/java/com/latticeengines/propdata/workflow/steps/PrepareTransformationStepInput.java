package com.latticeengines.propdata.workflow.steps;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("preparePropdataTransformationStepInput")
@Scope("prototype")
public class PrepareTransformationStepInput extends BaseWorkflowStep<PrepareTransformationStepInputConfiguration> {

    private static Log log = LogFactory.getLog(PrepareTransformationStepInput.class);
    private Schema schema;

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Value("${propdata.match.max.num.blocks:4}")
    private Integer maxNumBlocks;

    @Value("${propdata.match.num.threads:4}")
    private Integer threadPoolSize;

    @Value("${propdata.match.group.size:20}")
    private Integer groupSize;

    @Override
    public void execute() {
        log.info("Inside PrepareTransformationStepInput execute()");

        Properties configurations = new Properties();
        executionContext.put(BulkMatchContextKey.YARN_JOB_CONFIGS, configurations);
    }

}
