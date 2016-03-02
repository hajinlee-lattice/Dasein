package com.latticeengines.pls.workflow;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.leadprioritization.workflow.CreateModelWorkflowConfiguration;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component
public class CreateModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {
    private static final Logger log = Logger.getLogger(CreateModelWorkflowSubmitter.class);

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    public ApplicationId submit(ModelingParameters parameters) {
        SourceFile sourceFile = sourceFileService.findByName(parameters.getFilename());

        if (sourceFile == null) {
            throw new LedpException(LedpCode.LEDP_18084, new String[] { parameters.getFilename() });
        }

        if (hasRunningWorkflow(sourceFile)) {
            throw new LedpException(LedpCode.LEDP_18081, new String[] { sourceFile.getName() });
        }

        Table eventTable = metadataProxy.getTable(SecurityContextUtils.getCustomerSpace().toString(),
                sourceFile.getTableName());

        if (eventTable == null) {
            throw new LedpException(LedpCode.LEDP_18088, new String[] { parameters.getEventTableName() });
        }

        List<String> eventColumns = getEventColumns(eventTable);

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
                .eventTableName(sourceFile.getTableName()) //
                .build();
        AppSubmission submission = workflowProxy.submitWorkflowExecution(configuration);
        String applicationId = submission.getApplicationIds().get(0);
        sourceFile.setApplicationId(applicationId);
        sourceFileService.update(sourceFile);
        return YarnUtils.appIdFromString(applicationId);
    }

}
