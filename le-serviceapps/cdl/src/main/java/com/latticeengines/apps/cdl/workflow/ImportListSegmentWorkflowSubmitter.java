package com.latticeengines.apps.cdl.workflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeModule;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ListSegmentImportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.ImportListSegmentWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component
public class ImportListSegmentWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(ImportListSegmentWorkflowSubmitter.class);

    @Inject
    private BatonService batonService;

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Value("${aws.s3.data.stage.bucket}")
    private String dateStageBucket;

    @Value("${aws.customer.s3.bucket}")
    private String customerBucket;

    private String FILE_KEY = "FILE_KEY";

    @WithWorkflowJobPid
    public ApplicationId submit(@NotNull String customerSpace, @NotNull ListSegmentImportRequest request, @NotNull WorkflowPidWrapper pidWrapper) {
        Map<String, String> inputProperties = new HashMap<>();
        String tenantId = MultiTenantContext.getTenant().getId();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "importListSegmentWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.SEGMENT_NAME, request.getSegmentName());
        inputProperties.put(FILE_KEY, request.getS3FileKey());
        boolean entityMatchEnabled = batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace());
        Map<String, List<String>> systemIdMaps = getSystemIdMaps(customerSpace, entityMatchEnabled);
        ImportListSegmentWorkflowConfiguration configuration = new ImportListSegmentWorkflowConfiguration.Builder()
                .customer(CustomerSpace.parse(customerSpace))
                .sourceBucket(dateStageBucket)
                .sourceKey(request.getS3FileKey())
                .destBucket(customerBucket)
                .segmentName(request.getSegmentName())
                .inputProperties(inputProperties)
                .isSSVITenant(judgeTenantType(tenantId, LatticeModule.SSVI))
                .isCDLTenant(judgeTenantType(tenantId, LatticeModule.CDL))
                .systemIdMaps(systemIdMaps)
                .build();
        ApplicationId applicationId = workflowJobService.submit(configuration, pidWrapper.getPid());
        return applicationId;
    }

    private Map<String, List<String>> getSystemIdMaps(@NotNull String customerSpace, boolean entityMatchEnabled) {
        if (!entityMatchEnabled) {
            return Collections.emptyMap();
        }
        List<S3ImportSystem> systems = s3ImportSystemService.getAllS3ImportSystem(customerSpace);
        log.info("Systems={}, customerSpace={}", JsonUtils.serialize(systems), customerSpace);
        List<String> accountIds = getSystemIds(systems, BusinessEntity.Account);
        log.info("account ids={}, customerSpace={}", accountIds, customerSpace);
        Map<String, List<String>> systemIds = new HashMap<>();
        systemIds.put(BusinessEntity.Account.name(), accountIds);
        return systemIds;
    }

    private List<String> getSystemIds(@NotNull List<S3ImportSystem> systems, @NotNull BusinessEntity entity) {
        if (CollectionUtils.isEmpty(systems)) {
            return Collections.emptyList();
        }
        List<String> systemIds = systems.stream().filter(Objects::nonNull) //
                // sort by system priority (lower number has higher priority)
                .sorted(Comparator.comparing(S3ImportSystem::getPriority)) //
                .flatMap(sys -> getOneSystemIds(entity, sys).stream()) //
                .filter(StringUtils::isNotBlank) //
                .collect(Collectors.toList());
        return systemIds;
    }

    private List<String> getOneSystemIds(@NotNull BusinessEntity entity, @NotNull S3ImportSystem system) {
        List<String> allIds = new ArrayList<>();
        switch (entity) {
            case Account:
                if (StringUtils.isNotBlank(system.getAccountSystemId())) {
                    allIds.add(system.getAccountSystemId());
                }
                List<String> secondaryAccountIdList = system.getSecondaryAccountIdsSortByPriority();
                if (CollectionUtils.isNotEmpty(secondaryAccountIdList)) {
                    allIds.addAll(secondaryAccountIdList);
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Does not support retrieving system IDs for entity [%s]", entity.name()));
        }
        return allIds;
    }

    private boolean judgeTenantType(@NotNull String tenantId, LatticeModule latticeModule) {
        try {
            return batonService.hasModule(CustomerSpace.parse(tenantId), latticeModule);
        } catch (Exception e) {
            log.error("Failed to verify whether {} is a {}} tenant. error = {}", tenantId, latticeModule, e);
            return false;
        }
    }

}
