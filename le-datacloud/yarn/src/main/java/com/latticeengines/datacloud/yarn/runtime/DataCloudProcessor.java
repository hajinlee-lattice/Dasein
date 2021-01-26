package com.latticeengines.datacloud.yarn.runtime;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.aspect.MatchStepAspect;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.monitor.tracing.TracingTags;
import com.latticeengines.monitor.util.TracingUtils;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

@Component("dataCloudProcessor")
public class DataCloudProcessor extends SingleContainerYarnProcessor<DataCloudJobConfiguration> {

    private static final Logger logger = LoggerFactory.getLogger(DataCloudProcessor.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private MatchActorSystem matchActorSystem;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Inject
    private ProcessorContext initialProcessorContext;

    @Resource(name = "bulkMatchProcessorExecutor")
    private BulkMatchProcessorExecutor bulkMatchProcessorExecutor;

    @Resource(name = "bulkMatchProcessorAsyncExecutor")
    private BulkMatchProcessorExecutor bulkMatchProcessorAsyncExecutor;

    // Entry point for DataCloud job
    @Override
    public String process(DataCloudJobConfiguration jobConfiguration) throws Exception {
        Tracer tracer = GlobalTracer.get();
        Span workflowSpan = startDataCloudWorkflowSpan(jobConfiguration,
                TracingUtils.getSpanContext(jobConfiguration.getTracingContext()));
        try (Scope scope = tracer.activateSpan(workflowSpan)) {
            LogManager.getLogger(MatchStepAspect.class).setLevel(Level.DEBUG);
            matchActorSystem.setBatchMode(true);
            setAllocateModeFlag(jobConfiguration.getMatchInput());
            workflowSpan.log(getMatchDebugFields(jobConfiguration));

            initialProcessorContext.initialize(this, jobConfiguration);
            if (initialProcessorContext.isUseRemoteDnB()) {
                logger.info("Use async executor.");
                bulkMatchProcessorAsyncExecutor.execute(initialProcessorContext);
                bulkMatchProcessorAsyncExecutor.finalize(initialProcessorContext);
            } else {
                logger.info("Use sync executor.");
                bulkMatchProcessorExecutor.execute(initialProcessorContext);
                bulkMatchProcessorExecutor.finalize(initialProcessorContext);
            }

        } catch (Exception e) {
            String rootOperationUid = jobConfiguration.getRootOperationUid();
            String blockOperationUid = jobConfiguration.getBlockOperationUid();
            String errFile = hdfsPathBuilder.constructMatchBlockErrorFile(rootOperationUid, blockOperationUid)
                    .toString();
            try {
                HdfsUtils.writeToFile(yarnConfiguration, errFile, ExceptionUtils.getStackTrace(e));
            } catch (Exception e1) {
                logger.error("Failed to write error to err file.", e1);
            }

            TracingUtils.logError(workflowSpan, e,
                    String.format("Failed to execute dataCloudJob %s. error message = %s",
                            jobConfiguration.getAppName(), e.getMessage()));
            throw (e);
        } finally {
            TracingUtils.finish(workflowSpan);
        }

        return null;
    }

    private Map<String, String> getMatchDebugFields(DataCloudJobConfiguration config) {
        Map<String, String> fields = new HashMap<>();
        if (config.getMatchInput() != null) {
            fields.put("matchInput", JsonUtils.serialize(config.getMatchInput()));
        }
        if (StringUtils.isNotBlank(config.getMatchInputPath())) {
            fields.put("matchInputPath", config.getMatchInputPath());
        }
        if (StringUtils.isNotBlank(config.getYarnQueue())) {
            fields.put("yarnQueue", config.getYarnQueue());
        }
        return fields;
    }

    private Span startDataCloudWorkflowSpan(@NotNull DataCloudJobConfiguration config, SpanContext parentContext) {
        Tracer tracer = GlobalTracer.get();
        Tracer.SpanBuilder builder = tracer.buildSpan(config.getAppName()).addReference(References.FOLLOWS_FROM,
                parentContext);
        if (config.getRootOperationUid() != null) {
            builder.withTag(TracingTags.DataCloud.ROOT_OPERATION_UID, config.getRootOperationUid());
        }
        if (config.getBlockOperationUid() != null) {
            builder.withTag(TracingTags.DataCloud.BLOCK_OPERATION_UID, config.getBlockOperationUid());
        }
        return builder.start();
    }

    private void setAllocateModeFlag(MatchInput input) {
        boolean allocateMode = OperationalMode.isEntityMatch(input.getOperationalMode())
                && (input.isAllocateId() || input.isFetchOnly());
        if (allocateMode) {
            if (input.isAllocateId()) {
                logger.info("Entity match is in Allocate mode which generates EntityId during match");
            } else if (input.isFetchOnly()) {
                logger.info("Entity match is in FetchOnly mode "
                        + "which looks up seed by EntityId in staging table or serving table (if no found in staging table)");
            }
            entityMatchConfigurationService.setIsAllocateMode(allocateMode);
        }
        if (input.getEntityMatchConfiguration() != null
                && input.getEntityMatchConfiguration().getAllocateId() != null) {
            boolean allocationModeInConfig = input.getEntityMatchConfiguration().getAllocateId();
            logger.info("Overwrite allocate mode with value in entity match configuration. AllocateId = {}",
                    allocationModeInConfig);
            entityMatchConfigurationService.setIsAllocateMode(allocationModeInConfig);
        }
        if (input.getEntityMatchConfiguration() != null
                && MapUtils.isNotEmpty(input.getEntityMatchConfiguration().getAllocationModes())) {
            String srcEntity = input.getSourceEntity();
            if (StringUtils.isBlank(srcEntity)) {
                logger.info("Source entity is not set, use target entity {} instead", input.getTargetEntity());
                srcEntity = input.getTargetEntity();
            }
            EntityMatchUtils.setPerEntityAllocationModes(entityMatchConfigurationService,
                    input.getEntityMatchConfiguration(), srcEntity, true);
        }
    }

}
