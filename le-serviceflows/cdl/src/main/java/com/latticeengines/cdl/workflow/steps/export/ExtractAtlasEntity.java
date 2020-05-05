package com.latticeengines.cdl.workflow.steps.export;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATLAS_EXPORT;
import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;
import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.EXPORT_SCHEMA_MAP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.AccountContactExportContext;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.AccountContactExportConfig;
import com.latticeengines.proxy.exposed.cdl.AtlasExportProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.spark.exposed.job.cdl.AccountContactExportJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("extractAtlasEntity")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExtractAtlasEntity extends BaseSparkSQLStep<EntityExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExtractAtlasEntity.class);

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private AtlasExportProxy atlasExportProxy;

    @Inject
    private BatonService batonService;

    private DataCollection.Version version;
    private String evaluationDate;
    private AtlasExport atlasExport;
    private AttributeRepository attrRepo;
    private Map<BusinessEntity, List<ColumnMetadata>> schemaMap;
    private AccountContactExportContext accountContactExportContext = new AccountContactExportContext();
    private boolean entityMatchEnabled;

    @Override
    public void execute() {
        customerSpace = parseCustomerSpace(configuration);
        version = parseDataCollectionVersion(configuration);
        attrRepo = parseAttrRepo(configuration);
        evaluationDate = parseEvaluationDateStr(configuration);
        atlasExport = buildAtlasExport();
        schemaMap = getExportSchema();
        entityMatchEnabled = batonService.isEntityMatchEnabled(customerSpace);
        WorkflowStaticContext.putObject(EXPORT_SCHEMA_MAP, schemaMap);
        List<String> filesToDelete = new ArrayList<>();
        try {
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            Map<ExportEntity, HdfsDataUnit> resultMap = retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                    log.warn("Previous failure:", ctx.getLastThrowable());
                }
                Map<ExportEntity, HdfsDataUnit> resultForCurrentAttempt = new HashMap<>();
                try {
                    startSparkSQLSession(getHdfsPaths(attrRepo), false);
                    List<ExportEntity> entities = new ArrayList<>();
                    if (AtlasExportType.ACCOUNT.equals(atlasExport.getExportType())) {
                        entities.add(ExportEntity.Account);
                        resultForCurrentAttempt = getResultAttempt(entities);
                        addPathToDeletePath(filesToDelete, resultForCurrentAttempt);
                    } else if (AtlasExportType.CONTACT.equals(atlasExport.getExportType())) {
                        entities.add(ExportEntity.Contact);
                        resultForCurrentAttempt = getResultAttempt(entities);
                        addPathToDeletePath(filesToDelete, resultForCurrentAttempt);
                    } else if (AtlasExportType.ACCOUNT_AND_CONTACT.equals(atlasExport.getExportType())) {
                        entities.add(ExportEntity.Account);
                        entities.add(ExportEntity.Contact);
                        resultForCurrentAttempt = getResultAttempt(entities);
                        addPathToDeletePath(filesToDelete, resultForCurrentAttempt);
                        SparkJobResult sparkJobResult = executeSparkJob(AccountContactExportJob.class,
                                generateAccountAndContactExportConfig(resultForCurrentAttempt.get(ExportEntity.Account),
                                        resultForCurrentAttempt.get(ExportEntity.Contact)));
                        resultForCurrentAttempt = new HashMap<>();
                        HdfsDataUnit hdfsDataUnit = sparkJobResult.getTargets().get(0);
                        filesToDelete.add(hdfsDataUnit.getPath().substring(0, hdfsDataUnit.getPath().lastIndexOf("/")));
                        resultForCurrentAttempt.put(ExportEntity.AccountContact, hdfsDataUnit);
                    }
                } finally {
                    stopSparkSQLSession();
                }
                return resultForCurrentAttempt;
            });
            putObjectInContext(ATLAS_EXPORT_DATA_UNIT, resultMap);
            putObjectInContext(ATLAS_EXPORT_DELETE_PATH, filesToDelete);
        } catch (Exception e) {
            atlasExportProxy.updateAtlasExportStatus(customerSpace.toString(), atlasExport.getUuid(),
                    MetadataSegmentExport.Status.FAILED);
            throw new LedpException(LedpCode.LEDP_18167, e);
        }
    }

    private void addPathToDeletePath(List<String> files, Map<ExportEntity, HdfsDataUnit> outputUnits) {
        files.addAll(outputUnits.values().stream()
                .map(hdfsDataUnit -> hdfsDataUnit.getPath().substring(0, hdfsDataUnit.getPath().lastIndexOf("/")))
                .collect(Collectors.toList()));
    }

    private AtlasExport buildAtlasExport() {
        AtlasExport atlasExport = atlasExportProxy.findAtlasExportById(customerSpace.toString(),
                configuration.getAtlasExportId());
        WorkflowStaticContext.putObject(ATLAS_EXPORT, atlasExport);
        return atlasExport;
    }

    private AccountContactExportConfig generateAccountAndContactExportConfig(HdfsDataUnit accountDataUnit,
                                                                             HdfsDataUnit contactDataUnit) {
        AccountContactExportConfig accountContactExportConfig = new AccountContactExportConfig();
        accountContactExportConfig.setWorkspace(getRandomWorkspace());
        if (contactDataUnit != null) {
            accountContactExportConfig.setInput(Arrays.asList(accountDataUnit, contactDataUnit));
        } else {
            accountContactExportConfig.setInput(Collections.singletonList(accountDataUnit));
        }
        accountContactExportConfig.setAccountContactExportContext(accountContactExportContext);
        accountContactExportConfig.getAccountContactExportContext().setJoinKey(InterfaceName.AccountId.name());
        accountContactExportConfig.getAccountContactExportContext().setEntityMatchEnabled(entityMatchEnabled);
        log.info(String.format("workspace in account contact job is %s", accountContactExportConfig.getWorkspace()));
        return accountContactExportConfig;
    }

    private Map<ExportEntity, HdfsDataUnit> getResultAttempt(List<ExportEntity> exportEntities) {
        Map<ExportEntity, HdfsDataUnit> resultForCurrentAttempt = new HashMap<>();
        exportEntities.forEach(exportEntity -> {
            BusinessEntity mainEntity = null;
            if (ExportEntity.Account.equals(exportEntity)) {
                mainEntity = BusinessEntity.Account;
            } else if (ExportEntity.Contact.equals(exportEntity)) {
                mainEntity = BusinessEntity.Contact;
            }
            if (isEntityValid(mainEntity)) {
                FrontEndQuery frontEndQuery = new FrontEndQuery();
                frontEndQuery.setAccountRestriction(atlasExport.getAccountFrontEndRestriction());
                frontEndQuery.setContactRestriction(atlasExport.getContactFrontEndRestriction());
                HdfsDataUnit entityResult = exportOneEntity(exportEntity, frontEndQuery);
                resultForCurrentAttempt.put(exportEntity, entityResult);
            }
        });
        return resultForCurrentAttempt;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(EntityExportStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected DataCollection.Version parseDataCollectionVersion(EntityExportStepConfiguration stepConfiguration) {
        if (version == null) {
            version = configuration.getDataCollectionVersion();
        }
        return version;
    }

    @Override
    protected String parseEvaluationDateStr(EntityExportStepConfiguration stepConfiguration) {
        if (StringUtils.isBlank(evaluationDate)) {
            evaluationDate = periodProxy.getEvaluationDate(parseCustomerSpace(stepConfiguration).toString());
        }
        return evaluationDate;
    }

    @Override
    protected AttributeRepository parseAttrRepo(EntityExportStepConfiguration stepConfiguration) {
        AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
        if (attrRepo == null) {
            throw new RuntimeException("Cannot find attribute repo in context");
        }
        return attrRepo;
    }

    private void addAccountId(BusinessEntity businessEntity, List<List<ColumnMetadata>> metadataList,
                              String accountId) {
        ColumnMetadata columnMetadata = new ColumnMetadata();
        columnMetadata.setCategory(Category.ACCOUNT_ATTRIBUTES);
        columnMetadata.setAttrName(accountId);
        columnMetadata.setDisplayName(accountId);
        columnMetadata.setEntity(businessEntity);
        metadataList.get(Category.ACCOUNT_ATTRIBUTES.getOrder()).add(columnMetadata);
    }

    private void addContactId(BusinessEntity businessEntity, List<List<ColumnMetadata>> metadataList,
                              String contactId) {
        ColumnMetadata columnMetadata = new ColumnMetadata();
        columnMetadata.setCategory(Category.CONTACT_ATTRIBUTES);
        columnMetadata.setAttrName(contactId);
        columnMetadata.setDisplayName(contactId);
        columnMetadata.setEntity(businessEntity);
        metadataList.get(Category.CONTACT_ATTRIBUTES.getOrder()).add(columnMetadata);
    }

    private List<Lookup> convertToLookup(List<List<ColumnMetadata>> columnMetadataList) {
        List<Lookup> lookups = new ArrayList<>();
        columnMetadataList.forEach(cms -> cms.forEach(cm -> {
            lookups.add(new AttributeLookup(cm.getEntity(), cm.getAttrName()));
        }));
        return lookups;
    }

    private void determineAccountIdBoolValues(ColumnMetadata cm, MutableBoolean hasAccountId, String accountId,
                                              MutableBoolean hasInternalAccountId, String internalAccountId) {
        if (entityMatchEnabled) {
            if (accountId.equals(cm.getAttrName())) {
                hasAccountId.setTrue();
            }
            if (internalAccountId.equals(cm.getAttrName())) {
                hasInternalAccountId.setTrue();
            }
        } else {
            if (accountId.equals(cm.getAttrName())) {
                hasAccountId.setTrue();
                hasInternalAccountId.setTrue();
            }
        }
    }

    private List<Lookup> getAccountLookup() {
        List<List<ColumnMetadata>> columnMetadataList = new ArrayList<>();
        Arrays.stream(Category.values()).forEach(category -> columnMetadataList.add(new ArrayList<>()));
        MutableBoolean hasInternalAccountId = new MutableBoolean();
        MutableBoolean hasAccountId = new MutableBoolean();
        String internalAccountId = InterfaceName.AccountId.name();
        String accountId = entityMatchEnabled ? InterfaceName.CustomerAccountId.name() : InterfaceName.AccountId.name();
        for (BusinessEntity entity : BusinessEntity.EXPORT_ACCOUNT_ENTITIES) {
            List<ColumnMetadata> cms = schemaMap.getOrDefault(entity, Collections.emptyList());
            for (ColumnMetadata cm : cms) {
                determineAccountIdBoolValues(cm, hasAccountId, accountId, hasInternalAccountId, internalAccountId);
                columnMetadataList.get(cm.getCategory().getOrder()).add(cm);
            }
        }
        addAccountId(columnMetadataList, BusinessEntity.Account, hasInternalAccountId, hasAccountId, internalAccountId, accountId);
        sortAttribute(columnMetadataList);
        return convertToLookup(columnMetadataList);
    }

    private void addAccountId(List<List<ColumnMetadata>> columnMetadataList, BusinessEntity entity, MutableBoolean hasInternalAccountId,
                              MutableBoolean hasAccountId, String internalAccountId, String accountId) {
        if (atlasExport.getExportType().equals(AtlasExportType.ACCOUNT_AND_CONTACT)) {
            if (!hasInternalAccountId.booleanValue()) {
                addAccountId(entity, columnMetadataList, internalAccountId);
            }
            if (BusinessEntity.Account.equals(entity) && entityMatchEnabled) {
                if (!hasAccountId.booleanValue()) {
                    addAccountId(entity, columnMetadataList, accountId);
                }
            }
        } else {
            if (!hasAccountId.booleanValue()) {
                addAccountId(entity, columnMetadataList, accountId);
            }
        }
    }

    private List<Lookup> getContactLookup() {
        List<List<ColumnMetadata>> columnMetadataList = new ArrayList<>();
        Arrays.stream(Category.values()).forEach(category -> columnMetadataList.add(new ArrayList<>()));
        MutableBoolean hasAccountId = new MutableBoolean();
        MutableBoolean hasInternalAccountId = new MutableBoolean();
        String accountId = entityMatchEnabled ? InterfaceName.CustomerAccountId.name() : InterfaceName.AccountId.name();
        String internalAccountId = InterfaceName.AccountId.name();
        String contactId = entityMatchEnabled ? InterfaceName.CustomerContactId.name() : InterfaceName.ContactId.name();
        boolean hasContactId = false;
        for (BusinessEntity entity : BusinessEntity.EXPORT_CONTACT_ENTITIES) {
            List<ColumnMetadata> cms = schemaMap.getOrDefault(entity, Collections.emptyList());
            for (ColumnMetadata cm : cms) {
                determineAccountIdBoolValues(cm, hasAccountId, accountId, hasInternalAccountId, internalAccountId);
                if (contactId.equals(cm.getAttrName())) {
                    hasContactId = true;
                }
                columnMetadataList.get(cm.getCategory().getOrder()).add(cm);
            }
        }
        if (!hasContactId) {
            addContactId(BusinessEntity.Contact, columnMetadataList, contactId);
        }
        addAccountId(columnMetadataList, BusinessEntity.Contact, hasInternalAccountId, hasAccountId, internalAccountId, accountId);
        sortAttribute(columnMetadataList);
        return convertToLookup(columnMetadataList);
    }

    // sort display name for look up
    private void sortAttribute(List<List<ColumnMetadata>> columnMetadataList) {
        columnMetadataList.forEach(cms -> cms.sort((cm1, cm2) -> {
            String displayName1 = cm1.getDisplayName();
            if (StringUtils.isEmpty(displayName1)) {
                return -1;
            }
            String displayName2 = cm2.getDisplayName();
            if (StringUtils.isEmpty(displayName2)) {
                return 1;
            }
            return displayName1.compareToIgnoreCase(displayName2);
        }));
    }

    private HdfsDataUnit exportOneEntity(ExportEntity exportEntity, FrontEndQuery frontEndQuery) {
        List<Lookup> lookups;
        if (ExportEntity.Account.equals(exportEntity)) {
            frontEndQuery.setMainEntity(BusinessEntity.Account);
            lookups = getAccountLookup();
        } else if (ExportEntity.Contact.equals(exportEntity)) {
            frontEndQuery.setMainEntity(BusinessEntity.Contact);
            lookups = getContactLookup();
        } else {
            throw new UnsupportedOperationException("Unknown export entity " + exportEntity);
        }
        log.info("Going to export " + lookups.size() + " columns for " + exportEntity);
        frontEndQuery.setLookups(lookups);
        return getEntityQueryData(frontEndQuery);
    }

    private boolean isEntityValid(BusinessEntity mainEntity) {
        if (mainEntity == null) {
            return false;
        }
        TableRoleInCollection tableRole = mainEntity.getServingStore();
        if (tableRole == null) {
            log.warn("Cannot find a serving store for " + mainEntity);
            return false;
        }
        String tableName = attrRepo.getTableName(tableRole);
        if (tableName == null) {
            log.warn("Cannot find table of role " + tableRole + " in the repository.");
            return false;
        }
        return true;
    }

    private Map<BusinessEntity, List<ColumnMetadata>> getExportSchema() {
        List<ColumnSelection.Predefined> groups = Collections.singletonList(ColumnSelection.Predefined.Enrichment);
        Map<BusinessEntity, List<ColumnMetadata>> schemaMap = new HashMap<>();
        Set<BusinessEntity> entitySet = new HashSet<>(BusinessEntity.EXPORT_ACCOUNT_ENTITIES);
        entitySet.addAll(BusinessEntity.EXPORT_CONTACT_ENTITIES);
        for (BusinessEntity entity : entitySet) {
            List<ColumnMetadata> cms;
            if (StringUtils.isNotEmpty(atlasExport.getAttributeSetName())) {
                cms = servingStoreProxy.getDecoratedMetadata(customerSpace.toString(), entity, groups,
                        atlasExport.getAttributeSetName(), version).collectList().block();
                if (CollectionUtils.isNotEmpty(cms)) {
                    schemaMap.put(entity, cms);
                }
            } else {
                cms = servingStoreProxy.getDecoratedMetadata(customerSpace.toString(), entity, groups, version).collectList().block();
                if (CollectionUtils.isNotEmpty(cms)) {
                    schemaMap.put(entity, cms);
                }
            }
            log.info("Found " + CollectionUtils.size(cms) + " attrs to export for " + entity);
        }
        return schemaMap;
    }

}
