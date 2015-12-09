package com.latticeengines.pls.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.pls.service.TargetMarketService;
import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflowConfiguration;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component
public class WorkflowSubmitter {
    private static final Log log = LogFactory.getLog(WorkflowSubmitter.class);

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TargetMarketService targetMarketService;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Value("${pls.api.hostport}")
    private String internalResourceHostPort;

    @Value("${pls.microservice.rest.endpoint.hostport}")
    private String microserviceHostPort;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${pls.fitflow.stoplist.path}")
    private String stoplistPath;

    public void submitFitWorkflow(TargetMarket targetMarket) {
        String customer = SecurityContextUtils.getTenant().getId();
        log.info(String.format("Submitting fit model workflow for target market %s and customer %s",
                targetMarket.getName(), customer));
        try {
            List<String> eventCols = new ArrayList<>();
            eventCols.add("Event_IsWon");
            eventCols.add("Event_StageIsClosedWon");
            eventCols.add("Event_IsClosed");
            eventCols.add("Event_OpportunityCreated");

            Map<String, String> extraSources = new HashMap<>();
            extraSources.put("PublicDomain", stoplistPath);

            MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient();

            FitModelWorkflowConfiguration configuration = new FitModelWorkflowConfiguration.Builder()
                    .customer(CustomerSpace.parse(customer)) //
                    .microServiceHostPort(microserviceHostPort) //
                    .sourceType(SourceType.SALESFORCE) //
                    .targetPath("/FitModelRun") //
                    .extraSources(extraSources) //
                    .matchDbUrl(matchClientDocument.getUrl()) //
                    .matchDbUser(matchClientDocument.getUsername()) //
                    .matchDbPasswordEncrypted(matchClientDocument.getEncryptedPassword()) //
                    .internalResourceHostPort(internalResourceHostPort) //
                    .matchDestTables("DerivedColumns") //
                    .targetMarket(targetMarket) //
                    .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                    .matchClient(matchClientDocument.getMatchClient().name()) //
                    .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                    .eventColumns(eventCols) //
                    .build();

            String payloadName = "fitModelWorkflow" + "-" + customer + "-" + targetMarket.getName();
            configuration.setContainerConfiguration("fitModelWorkflow", CustomerSpace.parse(customer), payloadName);

            AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
            ApplicationId applicationId = YarnUtils.appIdFromString(submission.getApplicationIds().get(0));
            log.info(String.format("Submitted fit model workflow with application id %s", applicationId));
            targetMarket.setApplicationId(applicationId.toString());
            targetMarketService.updateTargetMarketByName(targetMarket, targetMarket.getName());
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                    "Failed to submit fit workflow for target market %s and customer %s", targetMarket.getName(),
                    customer), e);
        }
    }
}
