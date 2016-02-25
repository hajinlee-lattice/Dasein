package com.latticeengines.pls.workflow;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.SemanticType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.leadprioritization.workflow.CreateModelWorkflowConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import com.latticeengines.workflow.exposed.service.SourceFileService;

@Component
public class CreateModelWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = Logger.getLogger(CreateModelWorkflowSubmitter.class);

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    public ApplicationId submit(SourceFile sourceFile, ModelingParameters parameters) {
        if (hasRunningWorkflow(sourceFile)) {
            throw new LedpException(LedpCode.LEDP_18081, new String[] { sourceFile.getName() });
        }

        List<String> eventColumns = getEventColumns(sourceFile);

        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        CreateModelWorkflowConfiguration configuration = new CreateModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .sourceFileName(sourceFile.getName()) //
                .sourceType(SourceType.FILE) //
                .internalResourceHostPort(internalResourceHostPort) //
                .reportName(sourceFile.getName() + "_Report") //
                .dedupDataFlowBeanName("dedupEventTable") //
                .dedupDataFlowParams(new DedupEventTableParameters(sourceFile.getTableName())) //
                .dedupTargetTableName(sourceFile.getTableName() + "_deduped") //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .matchClientDocument(matchClientDocument) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumns") //
                .modelName(parameters.getName()) //
                .eventColumns(eventColumns) //
                .build();
        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
        String applicationId = submission.getApplicationIds().get(0);
        sourceFile.setApplicationId(applicationId);
        sourceFileService.update(sourceFile);
        return YarnUtils.appIdFromString(applicationId);
    }

    private List<String> getEventColumns(SourceFile sourceFile) {
        Table table = metadataProxy.getTable( //
                CustomerSpace.parse(SecurityContextUtils.getTenant().getId()).toString(), //
                sourceFile.getTableName());
        if (table == null) {
            log.error(String.format("No such table found with name %s for SourceFile with name %s",
                    sourceFile.getTableName(), sourceFile.getName()));
            return new ArrayList<>();
        }
        List<Attribute> events = table.findAttributes(new Predicate<Attribute>() {
            @Override
            public boolean apply(@Nullable Attribute attribute) {
                return attribute.getSemanticType() != null && attribute.getSemanticType() == SemanticType.Event;
            }
        });

        List<String> eventColumnNames = new ArrayList<>();
        for (Attribute column : events) {
            eventColumnNames.add(column.getName());
        }

        return eventColumnNames;
    }
}
