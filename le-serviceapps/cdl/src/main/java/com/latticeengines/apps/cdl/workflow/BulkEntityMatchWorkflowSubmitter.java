package com.latticeengines.apps.cdl.workflow;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.BulkEntityMatchRequest;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkEntityMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

/*
 * Submitter for BulkEntityMatchWorkflow
 */
@Component("bulkEntityMatchWorkflowSubmitter")
public class BulkEntityMatchWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(BulkEntityMatchWorkflowSubmitter.class);

    @Value("${datacloud.match.bulk.group.size:20}")
    private Integer groupSize;

    @Value("${datacloud.match.average.block.size:2500}")
    private Integer averageBlockSize;

    @Value("${datacloud.yarn.container.mem.mb}")
    private int yarnContainerMemory;

    @Value("${common.microservice.url}")
    private String microServiceHostPort;

    @Value("${datacloud.match.entity.test.input.s3.bucket}")
    private String s3Bucket;

    @Value("${datacloud.match.entity.test.input.s3.dir}")
    private String s3InputFileDir;

    @Value("${datacloud.match.entity.test.result.s3.bucket}")
    private String s3ResultBucket;

    @Value("${datacloud.match.entity.test.result.s3.dir}")
    private String s3ResultDir;

    // dir under tenant directory
    @Value("${datacloud.match.entity.test.input.hdfs.dir}")
    private String hdfsInputFileDir;

    @WithWorkflowJobPid
    public Response submit(@NotNull String customerSpace, @NotNull BulkEntityMatchRequest request,
            @NotNull WorkflowPidWrapper pidWrapper) {
        Preconditions.checkNotNull(customerSpace);
        check(request);
        Preconditions.checkNotNull(pidWrapper);
        fillDefaultValues(request, customerSpace);

        String entity = request.getEntity();
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        String uid = UUID.randomUUID().toString();

        BulkEntityMatchWorkflowConfiguration entityConfig = configureSteps(
                new BulkEntityMatchWorkflowConfiguration.Builder(entity, space), request, space, uid).build();

        log.debug("BulkEntityMatch config = {}", entityConfig);
        return getResponse(workflowJobService.submit(entityConfig, pidWrapper.getPid()), uid);
    }

    private Response getResponse(@NotNull ApplicationId applicationId, @NotNull String uid) {
        String resultPath = Paths.get(s3ResultDir, uid).toString();
        // NOTE location format = $bucket://$path
        String resultLocation = String.format("%s://%s", s3ResultBucket, resultPath);
        return new Response(applicationId.toString(), resultLocation);
    }

    private BulkEntityMatchWorkflowConfiguration.Builder configureSteps(
            @NotNull BulkEntityMatchWorkflowConfiguration.Builder builder, @NotNull BulkEntityMatchRequest request,
            @NotNull CustomerSpace customerSpace, @NotNull String uid) {
        // configure bump up version step
        if (request.getBumpVersion() != null) {
            BulkEntityMatchRequest.BumpVersionRequest bumpVersionRequest = request.getBumpVersion();
            if (CollectionUtils.isNotEmpty(bumpVersionRequest.getStagingCustomerSpaces())) {
                builder.bumpUpVersion(EntityMatchEnvironment.STAGING,
                        bumpVersionRequest.getStagingCustomerSpaces().toArray(new String[0]));
            }
            if (CollectionUtils.isNotEmpty(bumpVersionRequest.getServingCustomerSpaces())) {
                builder.bumpUpVersion(EntityMatchEnvironment.SERVING,
                        bumpVersionRequest.getServingCustomerSpaces().toArray(new String[0]));
            }
        }

        // configure entity publish steps
        if (request.getBeforeMatchPublish() != null) {
            builder.publishBeforeMatch(request.getBeforeMatchPublish());
        }
        if (request.getAfterMatchPublish() != null) {
            builder.publishAfterMatch(request.getAfterMatchPublish());
        }

        // configure bulk match step
        if (request.getBulkEntityMatchInput() != null) {
            String podId = CamilleEnvironment.getPodId();
            if (CollectionUtils.isNotEmpty(request.getInputFilePaths())) {
                HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();
                String atlasDir = pathBuilder.getHdfsAtlasDir(podId, customerSpace.getTenantId());
                String dir = Paths.get(atlasDir, hdfsInputFileDir, uid).toString();
                List<String> inputFilePaths = request.getInputFilePaths() //
                        .stream() //
                        .map(path -> FilenameUtils.concat(s3InputFileDir, path)) //
                        .collect(Collectors.toList());
                builder.copyTestFile(s3Bucket, inputFilePaths, dir);
                builder.cleanup(dir);

                // update bulk match input
                AvroInputBuffer buf = new AvroInputBuffer();
                buf.setAvroDir(dir);
                request.getBulkEntityMatchInput().setInputBuffer(buf);
            }
            BulkMatchWorkflowConfiguration config = new BulkMatchWorkflowConfiguration.Builder() //
                    .hdfsPodId(podId) //
                    .customer(customerSpace) //
                    .microserviceHostPort(microServiceHostPort) //
                    .averageBlockSize(averageBlockSize) //
                    .containerMemoryMB(yarnContainerMemory) //
                    .rootOperationUid(uid) //
                    .matchInput(request.getBulkEntityMatchInput()) //
                    .inputProperties(
                            Collections.singletonMap(WorkflowContextConstants.Inputs.JOB_TYPE, "bulkMatchWorkflow"))
                    .build();
            builder.bulkMatch(config);
            builder.podId(podId).rootOperationUid(uid);
        }

        return builder;
    }

    /*
     * Basic validation for request
     */
    private void check(@NotNull BulkEntityMatchRequest request) {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(request.getEntity());

        check(request.getEntity(), request.getBeforeMatchPublish());
        check(request.getEntity(), request.getAfterMatchPublish());
        check(request.getBumpVersion());
        check(request.getEntity(), request.getBulkEntityMatchInput());
        if (CollectionUtils.isNotEmpty(request.getInputFilePaths())) {
            request.getInputFilePaths().forEach(StringUtils::isNotBlank);
            Preconditions.checkNotNull(request.getBulkEntityMatchInput());
        }
    }

    private void check(@NotNull String entity, MatchInput input) {
        if (input == null) {
            return;
        }

        Preconditions.checkArgument(OperationalMode.isEntityMatch(input.getOperationalMode()));
        Preconditions.checkArgument(input.isSkipKeyResolution());
        Preconditions.checkArgument(input.getTargetEntity() == null || input.getTargetEntity().equals(entity),
                "Entity in MatchInput should be the same as the one in BulkEntityMatchRequest");
    }

    private void check(BulkEntityMatchRequest.BumpVersionRequest request) {
        if (request == null) {
            return;
        }

        if (CollectionUtils.isNotEmpty(request.getServingCustomerSpaces())) {
            request.getServingCustomerSpaces().forEach(Preconditions::checkNotNull);
        }
        if (CollectionUtils.isNotEmpty(request.getStagingCustomerSpaces())) {
            request.getStagingCustomerSpaces().forEach(Preconditions::checkNotNull);
        }
    }

    private void check(@NotNull String entity, EntityPublishRequest request) {
        if (request == null) {
            return;
        }

        Preconditions.checkNotNull(request.getSrcTenant());
        Preconditions.checkNotNull(request.getDestTenant());
        Preconditions.checkNotNull(request.getDestEnv());
        Preconditions.checkArgument(request.getEntity() == null || request.getEntity().equals(entity),
                "Entity in EntityPublishRequest should be the same as the one in BulkEntityMatchRequest");
    }

    private void fillDefaultValues(@NotNull BulkEntityMatchRequest request, @NotNull String customerSpace) {
        // filling entity for steps
        if (request.getBeforeMatchPublish() != null && request.getBeforeMatchPublish().getEntity() == null) {
            request.getBeforeMatchPublish().setEntity(request.getEntity());
        }
        if (request.getAfterMatchPublish() != null && request.getAfterMatchPublish().getEntity() == null) {
            request.getAfterMatchPublish().setEntity(request.getEntity());
        }
        if (request.getBulkEntityMatchInput() != null) {
            MatchInput matchInput = request.getBulkEntityMatchInput();
            if (matchInput.getTargetEntity() == null) {
                matchInput.setTargetEntity(request.getEntity());
            }
            if (matchInput.getTenant() == null) {
                matchInput.setTenant(new Tenant(CustomerSpace.parse(customerSpace).toString()));
            }
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Response {
        @JsonProperty("ApplicationId")
        private final String applicationId;

        @JsonProperty("ResultLocation")
        private final String resultLocation;

        public Response(String applicationId, String resultLocation) {
            this.applicationId = applicationId;
            this.resultLocation = resultLocation;
        }

        public String getApplicationId() {
            return applicationId;
        }

        public String getResultLocation() {
            return resultLocation;
        }
    }
}
