package com.latticeengines.cdl.workflow.steps.export;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATLAS_EXPORT;
import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;
import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.EXPORT_SCHEMA_MAP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
        schemaMap = getExportSchema();
        atlasExport = buildAtlasExport();
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
        files.addAll(outputUnits.values().stream().map(hdfsDataUnit -> hdfsDataUnit.getPath().substring(0, hdfsDataUnit.getPath().lastIndexOf("/"))).collect(Collectors.toList()));
    }

    private AtlasExport buildAtlasExport() {
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        AtlasExport atlasExport = atlasExportProxy.findAtlasExportById(customerSpaceStr,
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
        accountContactExportConfig.getAccountContactExportContext().setJoinKey(entityMatchEnabled ? InterfaceName.CustomerAccountId.name() : InterfaceName.AccountId.name());
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

    private void addAcoountId(BusinessEntity businessEntity, List<Lookup> lookups, String accountId) {
        AttributeLookup idLookup = new AttributeLookup(businessEntity, accountId);
        if (!lookups.contains(idLookup)) {
            lookups.add(idLookup);
        }
    }

    private void addContactId(BusinessEntity businessEntity, List<Lookup> lookups, String contactId) {
        AttributeLookup idLookup = new AttributeLookup(businessEntity, contactId);
        if (!lookups.contains(idLookup)) {
            lookups.add(idLookup);
        }
    }

    private List<Lookup> getAccountLookup() {
        List<Lookup> lookups = new ArrayList<>();
        for (BusinessEntity entity : BusinessEntity.EXPORT_ENTITIES) {
            if (!BusinessEntity.Contact.equals(entity)) {
                List<ColumnMetadata> cms = schemaMap.getOrDefault(entity, Collections.emptyList());
                cms.forEach(cm -> lookups.add(new AttributeLookup(entity, cm.getAttrName())));
            }
        }
        // when the export type is ACCOUNT_AND_CONTACT, needs to set the join used by AccountContactExportJob
        boolean hasAccountId = false;
        String accountId = entityMatchEnabled ? InterfaceName.CustomerAccountId.name() : InterfaceName.AccountId.name();
        if (AtlasExportType.ACCOUNT_AND_CONTACT.equals(atlasExport.getExportType())) {
            hasAccountId = true;
            addAcoountId(BusinessEntity.Account, lookups, accountId);
        }
        if (!hasAccountId) {
            addAcoountId(BusinessEntity.Account, lookups, accountId);
        }
        sortAttribute(lookups, accountId);
        return lookups;
    }

    private List<Lookup> getContactLookup() {
        List<ColumnMetadata> cms = schemaMap.getOrDefault(BusinessEntity.Contact, Collections.emptyList());
        List<Lookup> lookups = new ArrayList<>();
        boolean hasAccountId = false;
        String accountId = entityMatchEnabled ? InterfaceName.CustomerAccountId.name() : InterfaceName.AccountId.name();
        String contactId = entityMatchEnabled ? InterfaceName.CustomerContactId.name() : InterfaceName.ContactId.name();
        if (atlasExport.getExportType().equals(AtlasExportType.ACCOUNT_AND_CONTACT)) {
            cms.forEach(cm -> lookups.add(new AttributeLookup(BusinessEntity.Contact, cm.getAttrName())));
            // needs to add account id for left join operation later
            hasAccountId = true;
            addAcoountId(BusinessEntity.Contact, lookups, accountId);
        } else {
            cms.forEach(cm -> lookups.add(new AttributeLookup(BusinessEntity.Contact, cm.getAttrName())));
        }
        if (!hasAccountId) {
            addAcoountId(BusinessEntity.Contact, lookups, accountId);
        }
        addContactId(BusinessEntity.Contact, lookups, contactId);
        sortAttribute(lookups, contactId);
        return lookups;
    }

    private void sortAttribute(List<Lookup> lookups, String idName){
        lookups.sort((lookup1, lookup2) -> {
            AttributeLookup attributeLookup1 = (AttributeLookup) lookup1;
            if (idName.equals(attributeLookup1.getAttribute())) {
                return -1;
            }
            AttributeLookup attributeLookup2 = (AttributeLookup) lookup2;
            if (idName.equals(attributeLookup2.getAttribute())) {
                return 1;
            }
            return attributeLookup1.getAttribute().compareTo(attributeLookup2.getAttribute());
        });
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
        for (BusinessEntity entity : BusinessEntity.EXPORT_ENTITIES) {
            List<ColumnMetadata> cms = servingStoreProxy //
                    .getDecoratedMetadata(customerSpace.toString(), entity, groups, version).collectList().block();
            if (CollectionUtils.isNotEmpty(cms)) {
                schemaMap.put(entity, cms);
            }
            log.info("Found " + CollectionUtils.size(cms) + " attrs to export for " + entity);
        }
        return schemaMap;
    }

}

