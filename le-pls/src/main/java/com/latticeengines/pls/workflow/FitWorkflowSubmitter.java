package com.latticeengines.pls.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.TargetMarketService;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.FitModelWorkflowConfiguration;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class FitWorkflowSubmitter extends BaseModelWorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(FitWorkflowSubmitter.class);

    @Autowired
    private TargetMarketService targetMarketService;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Value("${pls.fitflow.stoplist.path}")
    private String stoplistPath;

    @Value("${pls.accountmaster.path}")
    private String accountMasterPath;

    private void checkForRunningWorkflow(TargetMarket targetMarket) {
        if (hasRunningWorkflow(targetMarket)) {
            throw new LedpException(LedpCode.LEDP_18076);
        }
    }

    public void submitWorkflowForTargetMarketAndWorkflowName(TargetMarket targetMarket, String workflowName) {
        checkForRunningWorkflow(targetMarket);

        String customer = MultiTenantContext.getCustomerSpace().toString();
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

            Map<String, String> inputProperties = new HashMap<>();
            inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "fitModelWorkflow");

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
                    .uniqueKeyColumn("LatticeAccountID") //
                    .directoryToScore(accountMasterPath) //
                    .registerScoredTable(true) //
                    .prematchFlowTableName("PrematchFlow") //
                    .attributes(
                            Arrays.asList(new String[] { "BusinessIndustry", "BusinessIndustry2",
                                    "BusinessRevenueRange", "BusinessEmployeesRange" })) //
                    .modelName("Default Model") //
                    .inputProperties(inputProperties) //
                    .build();

            String payloadName = workflowName + "-" + customer + "-" + targetMarket.getName();
            configuration.setContainerConfiguration(workflowName, CustomerSpace.parse(customer), payloadName);

            ApplicationId applicationId = workflowJobService.submit(configuration);
            targetMarket.setApplicationId(applicationId.toString());
            targetMarketService.updateTargetMarketByName(targetMarket, targetMarket.getName());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to submit %s for target market %s and customer %s",
                    workflowName, targetMarket.getName(), customer), e);
        }
    }
}
