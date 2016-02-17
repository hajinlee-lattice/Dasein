package com.latticeengines.pls.workflow;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.pls.service.TargetMarketService;
import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflowConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component
public class FitWorkflowSubmitter extends WorkflowSubmitter {
    private static final Log log = LogFactory.getLog(FitWorkflowSubmitter.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private TargetMarketService targetMarketService;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${pls.fitflow.stoplist.path}")
    private String stoplistPath;

    @Value("${pls.accountmaster.path}")
    private String accountMasterPath;

    private void checkForRunningWorkflow(TargetMarket targetMarket) {
        if (hasRunningWorkflow(targetMarket)) {
            throw new LedpException(LedpCode.LEDP_18076);
        }
    }

    public void submit(TargetMarket targetMarket) {
        checkForRunningWorkflow(targetMarket);

        String customer = SecurityContextUtils.getTenant().getId();
        log.info(String.format("Submitting fit model workflow for target market %s and customer %s",
                targetMarket.getName(), customer));
        try {
            metadataProxy.resetTables(customer);

            List<String> eventCols = new ArrayList<>();
            eventCols.add("Event_IsWon");
            eventCols.add("Event_StageIsClosedWon");
            eventCols.add("Event_IsClosed");
            eventCols.add("Event_OpportunityCreated");

            Map<String, String> extraSources = new HashMap<>();
            extraSources.put("PublicDomain", stoplistPath);

            MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

            FitModelWorkflowConfiguration configuration = new FitModelWorkflowConfiguration.Builder()
                    .customer(CustomerSpace.parse(customer)) //
                    .microServiceHostPort(microserviceHostPort) //
                    .sourceType(SourceType.SALESFORCE) //
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
                    .uniqueKeyColumn("LatticeAccountID") //
                    .directoryToScore(accountMasterPath) //
                    .registerScoredTable(true) //
                    .attributes(
                            Arrays.asList(new String[] { "BusinessIndustry", "BusinessIndustry2",
                                    "BusinessRevenueRange", "BusinessEmployeesRange" })) //
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
