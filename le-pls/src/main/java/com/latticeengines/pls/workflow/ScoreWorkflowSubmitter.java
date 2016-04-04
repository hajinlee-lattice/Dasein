package com.latticeengines.pls.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.leadprioritization.workflow.ScoreWorkflowConfiguration;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component
public class ScoreWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(ScoreWorkflowSubmitter.class);

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryService modelSummaryService;

    public ApplicationId submit(String modelId, String tableToScore) {
        log.info(String.format("Submitting score workflow for modelId %s and tableToScore %s for customer %s",
                modelId, tableToScore, SecurityContextUtils.getCustomerSpace()));
        ScoreWorkflowConfiguration configuration = generateConfiguration(modelId, tableToScore);

        if (metadataProxy.getTable(SecurityContextUtils.getCustomerSpace().toString(), tableToScore) == null) {
            throw new LedpException(LedpCode.LEDP_18098, new String[] { tableToScore });
        }

        if (!modelSummaryService.modelIdinTenant(modelId, SecurityContextUtils.getCustomerSpace().toString())) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        return workflowJobService.submit(configuration);
    }

    public ScoreWorkflowConfiguration generateConfiguration(String modelId, String tableToScore) {
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        return new ScoreWorkflowConfiguration.Builder() //
                .customer(SecurityContextUtils.getCustomerSpace()) //
                .matchClientDocument(matchClientDocument) //
                .microServiceHostPort(microserviceHostPort) //
                .modelId(modelId) //
                .inputTableName(tableToScore) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .build();
    }
}
