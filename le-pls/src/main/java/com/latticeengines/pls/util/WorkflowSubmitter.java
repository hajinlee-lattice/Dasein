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

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.pls.entitymanager.impl.microservice.RestApiProxy;
import com.latticeengines.pls.service.TargetMarketService;
import com.latticeengines.prospectdiscovery.workflow.FitModelWorkflowConfiguration;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component
public class WorkflowSubmitter {
    private static final Log log = LogFactory.getLog(WorkflowSubmitter.class);

    @Autowired
    private RestApiProxy restApiProxy;

    @Autowired
    private TargetMarketService targetMarketService;

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

        List<String> eventCols = new ArrayList<>();
        eventCols.add("Event_IsWon");
        eventCols.add("Event_StageIsClosedWon");
        eventCols.add("Event_IsClosed");
        eventCols.add("Event_OpportunityCreated");

        List<Map<String, String>> extraSources = new ArrayList<>();
        Map<String, String> entryMap = new HashMap<>();
        entryMap.put("PublicDomain", stoplistPath);
        extraSources.add(entryMap);
        try {
            FitModelWorkflowConfiguration configuration = new FitModelWorkflowConfiguration.Builder()
                    .customer(customer)
                    .microServiceHostPort(microserviceHostPort)
                    .sourceType(SourceType.SALESFORCE)
                    .targetPath("/FitModelRun")
                    .extraSources(extraSources)
                    // TODO get from API
                    .matchDbUrl(
                            "jdbc:sqlserver://10.51.15.130:1433;databaseName=PropDataMatchDB;user=DLTransfer;password=free&NSE")
                    .matchDbUser("DLTransfer") // TODO get from API
                    // TODO get from API
                    .matchDbPasswordEncrypted(CipherUtils.encrypt("free&NSE")) //
                    .matchDestTables("DerivedColumns") //
                    .targetMarket(targetMarket) //
                    .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                    .matchClient("PD130") // TODO get from API
                    .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                    .eventColumns(eventCols) //
                    .build();

            String payloadName = "fitModelWorkflow" + "-" + customer + "-" + targetMarket.getName();
            configuration
                    .setContainerConfiguration("fitModelWorkflow", CustomerSpace.parse(customer), payloadName);

            ApplicationId applicationId = restApiProxy.submitWorkflow(configuration);
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
