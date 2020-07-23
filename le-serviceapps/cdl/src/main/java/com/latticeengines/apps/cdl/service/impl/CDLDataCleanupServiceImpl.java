package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.EntityType.Accounts;
import static com.latticeengines.domain.exposed.query.EntityType.Contacts;
import static com.latticeengines.domain.exposed.query.EntityType.CustomIntent;
import static com.latticeengines.domain.exposed.query.EntityType.MarketingActivity;
import static com.latticeengines.domain.exposed.query.EntityType.Opportunity;
import static com.latticeengines.domain.exposed.query.EntityType.ProductPurchases;
import static com.latticeengines.domain.exposed.query.EntityType.WebVisit;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.apps.cdl.service.ActivityStoreService;
import com.latticeengines.apps.cdl.service.CDLDataCleanupService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.apps.cdl.workflow.CDLOperationWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.RegisterDeleteDataWorkflowSubmitter;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.DeleteRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.pls.LegacyDeleteByDateRangeActionConfiguration;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component("cdlDataCleanupService")
public class CDLDataCleanupServiceImpl implements CDLDataCleanupService {

    private static final Logger log = LoggerFactory.getLogger(CDLDataCleanupServiceImpl.class);

    private static final Map<EntityType, AtlasStream.StreamType> STREAM_TYPE_REFERENCE = ImmutableMap.of(WebVisit,
            AtlasStream.StreamType.WebVisit, Opportunity, AtlasStream.StreamType.Opportunity, MarketingActivity,
            AtlasStream.StreamType.MarketingActivity, CustomIntent, AtlasStream.StreamType.DnbIntentData);

    private static final Set<BusinessEntity> ACCOUNTS_CONTACTS = ImmutableSet.of(Account, BusinessEntity.Contact);
    private static final Set<BusinessEntity> ACCOUNTS = ImmutableSet.of(Account);
    // supported deletion date format, first one is used for standardization
    private static final List<DateTimeFormatter> DELETE_DATE_FORMATTERS = Arrays.asList(
            DateTimeFormatter.ofPattern(DateTimeUtils.DATE_ONLY_FORMAT_STRING),
            DateTimeFormatter.ofPattern("dd-MM-yyyy"));

    private static final Set<EntityType> TIME_SERIES_ENTITY_TYPES = ImmutableSet.of(ProductPurchases, WebVisit,
            Opportunity, MarketingActivity, CustomIntent);
    // entity type -> entities which IDs can be used to delete this type
    private static final Map<EntityType, Set<BusinessEntity>> ALLOW_DELETION_ENTITIES = ImmutableMap
            .<EntityType, Set<BusinessEntity>> builder() //
            .put(Accounts, ACCOUNTS) //
            .put(Contacts, ACCOUNTS_CONTACTS) //
            .put(ProductPurchases, ACCOUNTS) //
            .put(WebVisit, ACCOUNTS) //
            .put(Opportunity, ACCOUNTS) //
            .put(MarketingActivity, ACCOUNTS_CONTACTS) //
            .put(CustomIntent, ACCOUNTS) //
            .build();

    @Inject
    private TenantService tenantService;

    @Inject
    private ActionService actionService;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private ActivityStoreService activityStoreService;

    @Inject
    private RegisterDeleteDataWorkflowSubmitter registerDeleteDataWorkflowSubmitter;

    @Inject
    private BatonService batonService;

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    private final CDLOperationWorkflowSubmitter cdlOperationWorkflowSubmitter;

    @Inject
    public CDLDataCleanupServiceImpl(CDLOperationWorkflowSubmitter cdlOperationWorkflowSubmitter) {
        this.cdlOperationWorkflowSubmitter = cdlOperationWorkflowSubmitter;
    }

    @Override
    public ApplicationId cleanupData(String customerSpace, CleanupOperationConfiguration configuration) {
        log.info("customerSpace: " + customerSpace + ", CleanupOperationConfiguration: " + configuration);
        if (configuration instanceof CleanupByDateRangeConfiguration) {
            verifyCleanupByDataRangeConfiguration((CleanupByDateRangeConfiguration) configuration);
        }
        return cdlOperationWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), configuration,
                new WorkflowPidWrapper(-1L));
    }

    @Override
    public void createReplaceAction(String customerSpace, CleanupOperationConfiguration configuration) {
        //create delete action do not run workflow, waiting for PA to delete
        log.info("customerSpace: {}, CleanupOperationConfiguration: {}", customerSpace, configuration);
        log.info("Registering an operation action for tenant={}", customerSpace);
        Tenant tenant = tenantService.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace));
        }
        BusinessEntity businessEntity = configuration.getEntity();
        if (businessEntity == null) {
            createReplaceAction(tenant, configuration.getOperationInitiator(), Account);
            createReplaceAction(tenant, configuration.getOperationInitiator(),
                    BusinessEntity.Contact);
            createReplaceAction(tenant, configuration.getOperationInitiator(), BusinessEntity.Product);
            createReplaceAction(tenant, configuration.getOperationInitiator(), BusinessEntity.Transaction);
        } else {
            createReplaceAction(tenant, configuration.getOperationInitiator(), businessEntity);
        }
    }

    @Override
    public ApplicationId createLegacyDeleteUploadAction(String customerSpace, CleanupOperationConfiguration configuration) {
        if (isEntityMatchEnabled(customerSpace)) {
            throw new IllegalStateException("entityMatch tenant cannot create legacyDelete Action.");
        }
        String sourceFileName = ((CleanupByUploadConfiguration) configuration).getFileName();
        SourceFile sourceFile = sourceFileProxy.findByName(customerSpace, sourceFileName);
        if (sourceFile == null) {
            log.error("Cannot find SourceFile with name: {}", sourceFileName);
            throw new RuntimeException("Cannot find SourceFile with name: " + sourceFileName);
        }
        if (StringUtils.isEmpty(sourceFile.getTableName())) {
            log.error("SourceFile: {} does not have a table object!", sourceFileName);
            throw new RuntimeException(String.format("SourceFile: %s does not have a table object!", sourceFileName));
        }
        return registerDeleteDataWorkflowSubmitter.legacyDeleteSubmit(CustomerSpace.parse(customerSpace), sourceFile,
                configuration.getOperationInitiator(), configuration, new WorkflowPidWrapper(-1L));
    }

    @Override
    public void createLegacyDeleteDateRangeAction(String customerSpace, CleanupOperationConfiguration configuration) {
        if (isEntityMatchEnabled(customerSpace)) {
            throw new IllegalStateException("entityMatch tenant cannot create legacyDelete Action.");
        }
        log.info("customerSpace: {}, CleanupOperationConfiguration: {}", customerSpace, configuration);
        log.info("Registering an operation action for tenant={}", customerSpace);
        Tenant tenant = tenantService.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new NullPointerException(
                    String.format("Tenant with id=%s cannot be found", customerSpace));
        }
        if (configuration instanceof CleanupByDateRangeConfiguration) {
            BusinessEntity businessEntity = configuration.getEntity();
            if (businessEntity == null) {
                createLegacyDeleteByDateRangeAction(tenant, (CleanupByDateRangeConfiguration) configuration,
                        Account);
                createLegacyDeleteByDateRangeAction(tenant, (CleanupByDateRangeConfiguration) configuration,
                        BusinessEntity.Contact);
                createLegacyDeleteByDateRangeAction(tenant, (CleanupByDateRangeConfiguration) configuration,
                        BusinessEntity.Product);
                createLegacyDeleteByDateRangeAction(tenant, (CleanupByDateRangeConfiguration) configuration,
                        BusinessEntity.Transaction);
            } else {
                createLegacyDeleteByDateRangeAction(tenant, (CleanupByDateRangeConfiguration) configuration,
                        businessEntity);
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("Tenant with id=%s cannot find CleanupByDataRangeConfiguration when do delete " +
                            "operation.", customerSpace));
        }
    }

    @Override
    public ApplicationId registerDeleteData(String customerSpace, DeleteRequest request) {
        try {
            validateAndEnrichDeleteRequest(customerSpace, request);
        } catch (Exception e) {
            String msg = String.format("Failed to validate delete request %s for tenant %s",
                    JsonUtils.serialize(request), customerSpace);
            log.error(msg, e);
            throw e;
        }
        log.info("Delete request after standardization {}, tenant = {}", JsonUtils.serialize(request), customerSpace);
        String sourceFileName = request.getFilename();
        if (StringUtils.isBlank(sourceFileName)) {
            Action action = createTimeRangeDeleteAction(CustomerSpace.parse(customerSpace), request);
            log.info("Delete by time range action created successfully = {}", JsonUtils.serialize(action));
            // TODO maybe figure out a better fake ID
            return new FakeApplicationId(-1L);
        }
        SourceFile sourceFile = sourceFileProxy.findByName(customerSpace, sourceFileName);
        if (sourceFile == null) {
            log.error("Cannot find SourceFile with name: {}", sourceFileName);
            throw new RuntimeException("Cannot find SourceFile with name: " + sourceFileName);
        }
        if (StringUtils.isEmpty(sourceFile.getTableName())) {
            log.error("SourceFile: {} does not have a table object!", sourceFileName);
            throw new RuntimeException(String.format("SourceFile: %s does not have a table object!", sourceFileName));
        }
        request.setSourceFile(sourceFile);
        return registerDeleteDataWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), request, new WorkflowPidWrapper(-1L));
    }

    /*-
     * validate delete request and add additional information that is required by deletion job
     */
    private void validateAndEnrichDeleteRequest(@NotNull String customerSpace, @NotNull DeleteRequest request) {
        Preconditions.checkNotNull(request, "Delete request should not be null");
        EntityType deleteEntityType = request.getDeleteEntityType();
        BusinessEntity idEntity = request.getIdEntity();
        String idSystem = request.getIdSystem();
        String fromDate = request.getFromDate();
        String toDate = request.getToDate();
        // required params
        if (idEntity == null && StringUtils.isBlank(fromDate) && StringUtils.isBlank(toDate)) {
            throw new IllegalArgumentException(
                    "Delete request need to contain either id entity or date range to delete");
        }
        // supported entity for deletion
        if (deleteEntityType != null && !ALLOW_DELETION_ENTITIES.containsKey(deleteEntityType)) {
            String msg = String.format("Entity type %s is not supported for deletion", deleteEntityType);
            throw new IllegalArgumentException(msg);
        }
        validateDeleteIdEntity(idEntity, deleteEntityType, request.getFilename());
        validateDeleteIdSystem(customerSpace, idSystem, idEntity);
        validateAndStandardizeDeleteDateRange(request);
        addStreamIds(customerSpace, request);
        translateEntityTypeToDeleteEntities(request);
    }

    /*-
     * UI specify delete entity type, translate it into corresponding BusinessEntity
     * and add it to request (if not already if not already in it)
     */
    private void translateEntityTypeToDeleteEntities(@NotNull DeleteRequest request) {
        if (request.getDeleteEntityType() == null) {
            return;
        }

        if (request.getDeleteEntities() == null) {
            request.setDeleteEntities(new ArrayList<>());
        }
        BusinessEntity entity = request.getDeleteEntityType().getEntity();
        if (!request.getDeleteEntities().contains(entity)) {
            request.getDeleteEntities().add(entity);
        }
    }

    private void addStreamIds(@NotNull String customerSpace, @NotNull DeleteRequest request) {
        // delete ActivityStream indicated, prevent overwrite deleteStreamIds if already
        // set
        if (request.getDeleteEntityType() != null && STREAM_TYPE_REFERENCE.containsKey(request.getDeleteEntityType())
                && CollectionUtils.isEmpty(request.getDeleteStreamIds())) {
            request.setDeleteStreamIds(
                    translateStreamIds(CustomerSpace.parse(customerSpace), request.getDeleteEntityType()));
        }
    }

    private void validateAndStandardizeDeleteDateRange(@NotNull DeleteRequest request) {
        String fromDateStr = request.getFromDate();
        String toDateStr = request.getToDate();
        EntityType deleteEntityType = request.getDeleteEntityType();
        if (StringUtils.isBlank(fromDateStr) && StringUtils.isBlank(toDateStr)) {
            return;
        }
        if (StringUtils.isBlank(fromDateStr) || StringUtils.isBlank(toDateStr)) {
            // if one of ranges is provided, need to have both
            String msg = String.format("Missing date range %s for deletion",
                    StringUtils.isBlank(fromDateStr) ? "FromDate" : "ToDate");
            throw new IllegalArgumentException(msg);
        }
        if (deleteEntityType != null && !TIME_SERIES_ENTITY_TYPES.contains(deleteEntityType)) {
            String msg = String.format("%s is not a type of time series data and cannot be deleted by date range",
                    deleteEntityType);
            throw new IllegalArgumentException(msg);
        }

        LocalDate fromDate = parseDeleteDate(fromDateStr);
        LocalDate toDate = parseDeleteDate(toDateStr);
        if (fromDate.isAfter(toDate)) {
            String msg = String.format("Delete date range %s to %s is not valid", fromDateStr, toDateStr);
            throw new IllegalArgumentException(msg);
        }

        // standardize date format
        DateTimeFormatter formatter = DELETE_DATE_FORMATTERS.get(0);
        request.setFromDate(fromDate.format(formatter));
        request.setToDate(toDate.format(formatter));
    }

    private LocalDate parseDeleteDate(@NotNull String dateStr) {
        for (DateTimeFormatter formatter : DELETE_DATE_FORMATTERS) {
            try {
                return LocalDate.parse(dateStr, formatter);
            } catch (DateTimeParseException e) {
                // cannot parse, try another one, do nothing
                log.debug("Cannot parse {} using format {}, try the next one", dateStr, formatter);
            }
        }
        String msg = String.format(
                "Date string %s is not in supported format. Please use either yyyy-MM-dd or dd-MM-yyyy", dateStr);
        throw new IllegalArgumentException(msg);
    }

    private void validateDeleteIdSystem(@NotNull String customerSpace, String idSystem, BusinessEntity idEntity) {
        if (StringUtils.isNotBlank(idSystem)) {
            // validate specified system used for ID deletion
            Preconditions.checkNotNull(idEntity, "Need to specify entity of the ID used for deletion");
            S3ImportSystem sys = s3ImportSystemService.getS3ImportSystem(customerSpace, idSystem);
            if (sys == null) {
                String msg = String.format("Specified system %s does not exist", idSystem);
                throw new IllegalArgumentException(msg);
            }
            String mappedId = idEntity == Account ? sys.getAccountSystemId() : sys.getContactSystemId();
            if (StringUtils.isBlank(mappedId)) {
                String msg = String.format("No %s unique ID configured in system %s", idEntity, idSystem);
                throw new IllegalArgumentException(msg);
            }
        }
    }

    private void validateDeleteIdEntity(BusinessEntity idEntity, EntityType deleteEntityType, String deleteFilename) {
        if (idEntity != null && deleteEntityType != null
                && !ALLOW_DELETION_ENTITIES.get(deleteEntityType).contains(idEntity)) {
            // validate delete entity & id entity are compatible
            String msg = String.format("%s is not allowed to be deleted by %s ID", deleteEntityType,
                    idEntity.name().toLowerCase());
            throw new IllegalArgumentException(msg);
        }
        if (idEntity != null && StringUtils.isBlank(deleteFilename)) {
            // when delete by ID, need to provide a file
            String msg = String.format("Need to provide a file containing %s IDs that will be used for deletion",
                    idEntity.name().toLowerCase());
            throw new IllegalArgumentException(msg);
        }
        if (MarketingActivity.equals(deleteEntityType) && Account.equals(idEntity)
                && StringUtils.isNotBlank(deleteFilename)) {
            // FIXME remove this when delete contact level stream by account id is supported
            throw new UnsupportedOperationException(
                    "Delete marketing activity data by Account ID is not supported at the moment");
        }
    }

    private Action createTimeRangeDeleteAction(@NotNull CustomerSpace customerSpace, @NotNull DeleteRequest request) {
        Action action = new Action();
        action.setType(ActionType.SOFT_DELETE);
        action.setActionInitiator(request.getUser());
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        MultiTenantContext.setTenant(tenant);
        Preconditions.checkNotNull(tenant,
                String.format("Tenant with id=%s cannot be found", customerSpace.toString()));
        DeleteActionConfiguration deleteConfig = new DeleteActionConfiguration();
        deleteConfig.setDeleteEntities(request.getDeleteEntities());
        deleteConfig.setDeleteStreamIds(request.getDeleteStreamIds());
        deleteConfig.setDeleteEntityType(request.getDeleteEntityType());
        deleteConfig.setFromDate(request.getFromDate());
        deleteConfig.setToDate(request.getToDate());
        action.setActionConfiguration(deleteConfig);
        action.setTenant(tenant);
        return actionService.create(action);
    }

    private List<String> translateStreamIds(CustomerSpace customerSpace, EntityType deleteEntityType) {
        AtlasStream.StreamType streamType = STREAM_TYPE_REFERENCE.get(deleteEntityType);
        if (streamType == null) {
            throw new UnsupportedOperationException(String.format("entity type %s mapped to stream type %s is not supported for delete.", deleteEntityType, streamType));
        }
        List<AtlasStream> targetedStreams = activityStoreService.getStreamsByStreamType(customerSpace.toString(), streamType);
        return targetedStreams.stream().map(AtlasStream::getStreamId).collect(Collectors.toList());
    }

    private void verifyCleanupByDataRangeConfiguration(
            CleanupByDateRangeConfiguration cleanupByDateRangeConfiguration) {
        if (cleanupByDateRangeConfiguration.getStartTime() == null
                || cleanupByDateRangeConfiguration.getEndTime() == null) {
            throw new LedpException(LedpCode.LEDP_40002);
        }

        if (cleanupByDateRangeConfiguration.getStartTime().getTime() > cleanupByDateRangeConfiguration.getEndTime()
                .getTime()) {
            throw new LedpException(LedpCode.LEDP_40003);
        }
    }

    private void createReplaceAction(Tenant tenant, String operationInitiator, BusinessEntity businessEntity) {
        Action action = new Action();
        action.setType(ActionType.DATA_REPLACE);
        action.setActionInitiator(operationInitiator);
        CleanupActionConfiguration cleanupActionConfiguration = new CleanupActionConfiguration();
        cleanupActionConfiguration.addImpactEntity(businessEntity);
        action.setActionConfiguration(cleanupActionConfiguration);
        action.setTenant(tenant);
        if (tenant.getPid() != null) {
            MultiTenantContext.setTenant(tenant);
        } else {
            log.warn("The tenant in action does not have a pid:{}. ", tenant);
        }
        actionService.create(action);
    }

    private void createLegacyDeleteByDateRangeAction(Tenant tenant,
                                                     CleanupByDateRangeConfiguration cleanupByDateRangeConfiguration,
                                                     BusinessEntity entity) {
        Action action = new Action();
        action.setType(ActionType.LEGACY_DELETE_DATERANGE);
        action.setActionInitiator(cleanupByDateRangeConfiguration.getOperationInitiator());
        LegacyDeleteByDateRangeActionConfiguration legacyDeleteByDateRangeActionConfiguration =
                new LegacyDeleteByDateRangeActionConfiguration();
        legacyDeleteByDateRangeActionConfiguration.setEntity(entity);
        legacyDeleteByDateRangeActionConfiguration.setStartTime(cleanupByDateRangeConfiguration.getStartTime());
        legacyDeleteByDateRangeActionConfiguration.setEndTime(cleanupByDateRangeConfiguration.getEndTime());
        action.setActionConfiguration(legacyDeleteByDateRangeActionConfiguration);
        action.setTenant(tenant);
        if (tenant.getPid() != null) {
            MultiTenantContext.setTenant(tenant);
        } else {
            log.warn("The tenant in action does not have a pid:{}. ", tenant);
        }
        actionService.create(action);
    }

    private boolean isEntityMatchEnabled(String customerSpace) {
        return batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace)) && !batonService.onlyEntityMatchGAEnabled(CustomerSpace.parse(customerSpace));
    }

}
