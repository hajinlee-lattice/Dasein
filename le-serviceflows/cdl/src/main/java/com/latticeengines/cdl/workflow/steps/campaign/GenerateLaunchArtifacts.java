package com.latticeengines.cdl.workflow.steps.campaign;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.workflow.steps.campaign.utils.CampaignLaunchUtils;
import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.Play.TapType;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.GenerateLaunchArtifactsStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchArtifactsJobConfig;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.proxy.exposed.cdl.ExportFieldMetadataProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.query.util.AttrRepoUtils;
import com.latticeengines.spark.exposed.job.cdl.GenerateLaunchArtifactsJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("generateLaunchArtifacts")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateLaunchArtifacts extends BaseSparkSQLStep<GenerateLaunchArtifactsStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(GenerateLaunchArtifacts.class);

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ExportFieldMetadataProxy exportFieldMetadataProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private CampaignLaunchUtils campaignLaunchUtils;

    private Set<String> additionalContactAttr = new HashSet<>(Arrays.asList(InterfaceName.FirstName.name(),
            InterfaceName.LastName.name(), InterfaceName.Address_Street_2.name(), InterfaceName.DoNotCall.name(),
            InterfaceName.DoNotMail.name(), InterfaceName.CreatedDate.name(), InterfaceName.LastModifiedDate.name()));

    private DataCollection.Version version;
    private String evaluationDate;
    private AttributeRepository attrRepo;

    @Value("${cdl.outreach.max.launch}")
    private Long outreachMaxLaunch;

    @Value("${datacloud.manage.url}")
    private String url;

    @Value("${datacloud.manage.user}")
    private String user;

    @Value("${datacloud.manage.password.encrypted}")
    private String password;

    private boolean baseOnOtherTapType;

    @Override
    public void execute() {
        GenerateLaunchArtifactsStepConfiguration config = getConfiguration();
        parseCustomerSpace(config);
        Play play = playProxy.getPlay(customerSpace.getTenantId(), config.getPlayId(), false, false);
        PlayLaunchChannel channel = playProxy.getChannelById(customerSpace.getTenantId(), config.getPlayId(),
                config.getChannelId());
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[]{"No Campaign found by ID: " + config.getPlayId()});
        }
        if (channel == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[]{"No Channel found by ID: " + config.getChannelId()});
        }
        PlayLaunch launch = null;
        if (StringUtils.isNotBlank(config.getLaunchId())) {
            launch = playProxy.getPlayLaunch(customerSpace.getTenantId(), config.getPlayId(), config.getLaunchId());
            channel = playProxy.getPlayLaunchChannelFromPlayLaunch(customerSpace.getTenantId(), config.getPlayId(),
                    config.getLaunchId());
        }
        // check whether the step needs to be skipped
        if (shouldSkipStep(
                launch == null ? channel.getChannelConfig().getAudienceType()
                        : launch.getChannelConfig().getAudienceType(),
                channel.getLookupIdMap().getExternalSystemName())) {
            log.info("No Delta Data found, skipping Launch Artifact generation");
            return;
        }

        TapType tapType = play.getTapType();
        baseOnOtherTapType = TapType.ListSegment.equals(tapType);
        ChannelConfig channelConfig = launch == null ? channel.getChannelConfig() : launch.getChannelConfig();
        BusinessEntity mainEntity = channelConfig.getAudienceType().asBusinessEntity();
        boolean useContactsPerAccountLimit = hasContactsPerAccountLimit(channel, mainEntity);
        HdfsDataUnit perAccountLimitedContacts  = useContactsPerAccountLimit ? //
                getObjectFromContext(FULL_CONTACTS_UNIVERSE + ATLAS_EXPORT_DATA_UNIT, HdfsDataUnit.class) : new HdfsDataUnit();
        HdfsDataUnit positiveDeltaDataUnit = getObjectFromContext(
                getAddDeltaTableContextKeyByAudienceType(channelConfig.getAudienceType()) + ATLAS_EXPORT_DATA_UNIT, HdfsDataUnit.class);
        HdfsDataUnit negativeDeltaDataUnit = getObjectFromContext(
                getRemoveDeltaTableContextKeyByAudienceType(channelConfig.getAudienceType()) + ATLAS_EXPORT_DATA_UNIT, HdfsDataUnit.class);
        String accountLookupId = launch == null ? channel.getLookupIdMap().getAccountId() : launch.getDestinationAccountId();
        String contactLookupId = launch == null ? channel.getLookupIdMap().getContactId() : launch.getDestinationContactId();
        CDLExternalSystemName externalSystemName = launch == null ? channel.getLookupIdMap().getExternalSystemName()
                : launch.getDestinationSysName();
        log.info("externalSystemName=" + externalSystemName);

        List<ColumnMetadata> fieldMappingMetadata = exportFieldMetadataProxy.getExportFields(customerSpace.toString(),
                channel.getId());
        if (fieldMappingMetadata != null) {
            log.info("For tenant= " + config.getCustomerSpace().getTenantId() + ", playChannelId= " + channel.getId()
                    + ", the columnMetadata size is=" + fieldMappingMetadata.size());
        }

        Set<Lookup> accountLookups = buildLookupsByEntity(BusinessEntity.Account, fieldMappingMetadata, channel);
        if (StringUtils.isNotBlank(accountLookupId)) {
            accountLookups.add(new AttributeLookup(BusinessEntity.Account, accountLookupId));
        }
        Set<Lookup> contactLookups = buildLookupsByEntity(BusinessEntity.Contact, fieldMappingMetadata, channel);
        if (StringUtils.isNotBlank(contactLookupId)) {
            contactLookups.add(new AttributeLookup(BusinessEntity.Contact, contactLookupId));
        }
        if (campaignLaunchUtils.getUseCustomerId(customerSpace, externalSystemName)) {
            appendCustomerId(accountLookups, InterfaceName.CustomerAccountId, BusinessEntity.Account);
            appendCustomerId(contactLookups, InterfaceName.CustomerContactId, BusinessEntity.Contact);
        }
        SparkJobResult sparkJobResult;
        if (baseOnOtherTapType) {
            sparkJobResult = runSparkJob(accountLookups, contactLookups, positiveDeltaDataUnit, negativeDeltaDataUnit,
                    mainEntity, channelConfig.isSuppressAccountsWithoutContacts(), channel.getLookupIdMap().getExternalSystemName(),
                    perAccountLimitedContacts, useContactsPerAccountLimit);
        } else {
            version = parseDataCollectionVersion(configuration);
            attrRepo = parseAttrRepo(configuration);
            evaluationDate = parseEvaluationDateStr(configuration);
            boolean contactsDataExists = AttrRepoUtils.testExistsEntity(attrRepo, BusinessEntity.Contact);
            if (!contactsDataExists) {
                log.info("No Contact data found in the Attribute Repo");
            }
            accountLookups = addRatingLookups(play.getRatingEngine(), accountLookups);
            log.info("Account Lookups: " + accountLookups.stream().map(Lookup::toString).collect(Collectors.joining(", ")));
            log.info("Contact Lookups: " + contactLookups.stream().map(Lookup::toString).collect(Collectors.joining(", ")));
            sparkJobResult = executeSparkJob(play.getTargetSegment(), accountLookups, contactLookups,
                    positiveDeltaDataUnit, negativeDeltaDataUnit,
                    contactsDataExists ? mainEntity : BusinessEntity.Account,
                    contactsDataExists, channelConfig.isSuppressAccountsWithoutContacts(), channel.getLookupIdMap().getExternalSystemName(),
                    perAccountLimitedContacts, useContactsPerAccountLimit);
        }
        processSparkJobResults(channelConfig.getAudienceType(), channelConfig.getSystemName(), sparkJobResult);
    }

    private void appendCustomerId(Set<Lookup> lookups, InterfaceName customerId, BusinessEntity entity){
        boolean hasCustomerId = false;
        for (Lookup lookup : lookups) {
            AttributeLookup attributeLookup = (AttributeLookup) lookup;
            if (customerId.name().equals(attributeLookup.getAttribute())) {
                hasCustomerId = true;
            }
        }
        if (!hasCustomerId) {
            lookups.add(new AttributeLookup(entity, customerId.name()));
        }
    }

    private SparkJobResult runSparkJob(Set<Lookup> accountLookups, Set<Lookup> contactLookups, HdfsDataUnit positiveDeltaDataUnit, HdfsDataUnit negativeDeltaDataUnit,
                                       BusinessEntity mainEntity, boolean suppressAccountsWithoutContacts, CDLExternalSystemName externalSystemName,
                                       HdfsDataUnit perAccountLimitedContacts, boolean useContactsPerAccountLimit) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(2);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            HdfsDataUnit accountDataUnit = getObjectFromContext(ACCOUNTS_DATA_UNIT, HdfsDataUnit.class);
            HdfsDataUnit contactDataUnit = getObjectFromContext(CONTACTS_DATA_UNIT, HdfsDataUnit.class);
            GenerateLaunchArtifactsJobConfig config = getJobConfig(accountLookups, contactLookups, accountDataUnit, contactDataUnit,
                    contactDataUnit, negativeDeltaDataUnit, positiveDeltaDataUnit, mainEntity, suppressAccountsWithoutContacts, externalSystemName,
                    perAccountLimitedContacts, useContactsPerAccountLimit);
            log.info("Executing GenerateLaunchArtifactsJob with config: " + JsonUtils.serialize(config));
            SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
            log.info("GenerateLaunchArtifactsJob Results: " + JsonUtils.serialize(result));
            return result;
        });
    }

    private GenerateLaunchArtifactsJobConfig getJobConfig(Set<Lookup> accountLookups, Set<Lookup> contactLookups, HdfsDataUnit accountDataUnit,
                                                          HdfsDataUnit contactDataUnit, HdfsDataUnit targetContactsDataUnit,
                                                          HdfsDataUnit negativeDeltaDataUnit, HdfsDataUnit positiveDeltaDataUnit, BusinessEntity mainEntity,
                                                          boolean suppressAccountsWithoutContacts, CDLExternalSystemName externalSystemName,
                                                          HdfsDataUnit perAccountLimitedContacts, boolean useContactsPerAccountLimit) {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig(accountDataUnit, //
                contactDataUnit, targetContactsDataUnit, negativeDeltaDataUnit, positiveDeltaDataUnit, //
                mainEntity, !suppressAccountsWithoutContacts, getRandomWorkspace(), externalSystemName, //
                perAccountLimitedContacts, useContactsPerAccountLimit);
        config.setAccountAttributes(accountLookups.stream().map(lookup -> ((AttributeLookup)lookup).getAttribute()).collect(Collectors.toSet()));
        config.setContactAttributes(contactLookups.stream().map(lookup -> ((AttributeLookup)lookup).getAttribute()).collect(Collectors.toSet()));
        if (CDLExternalSystemName.AD_PLATFORMS.contains(externalSystemName)) {
            String encryptionKey = CipherUtils.generateKey();
            String saltHint = CipherUtils.generateKey();
            config.setManageDbUrl(url);
            config.setUser(user);
            config.setEncryptionKey(encryptionKey);
            config.setSaltHint(saltHint);
            config.setPassword(CipherUtils.encrypt(password, encryptionKey, saltHint));
        }
        return config;
    }

    private SparkJobResult executeSparkJob(MetadataSegment targetSegment, Set<Lookup> accountLookups, Set<Lookup> contactLookups, HdfsDataUnit positiveDeltaDataUnit,
                                           HdfsDataUnit negativeDeltaDataUnit, BusinessEntity mainEntity, boolean contactsDataExists, boolean suppressAccountsWithoutContacts,
                                           CDLExternalSystemName externalSystemName, HdfsDataUnit perAccountLimitedContacts, boolean useContactsPerAccountLimit) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(2);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);
                FrontEndQuery query = new FrontEndQuery();
                query.setLookups(new ArrayList<>(accountLookups));
                query.setMainEntity(BusinessEntity.Account);
                HdfsDataUnit accountDataUnit = getEntityQueryData(query);
                HdfsDataUnit contactDataUnit = null, targetContactsDataUnit = null;
                if (contactsDataExists) {
                    query.setLookups(new ArrayList<>(contactLookups));
                    query.setMainEntity(BusinessEntity.Contact);
                    contactDataUnit = getEntityQueryData(query);
                    FrontEndQuery targetContactsQuery = FrontEndQuery.fromSegment(targetSegment);
                    targetContactsQuery.setLookups(new ArrayList<>(contactLookups));
                    targetContactsQuery.setMainEntity(BusinessEntity.Contact);
                    targetContactsDataUnit = getEntityQueryData(targetContactsQuery);
                } else {
                    log.info("Ignoring Contact lookups since no contact data found in the Attribute Repo");
                }
                GenerateLaunchArtifactsJobConfig config = getJobConfig(accountLookups, contactLookups, accountDataUnit, contactDataUnit,
                        targetContactsDataUnit, negativeDeltaDataUnit, positiveDeltaDataUnit, mainEntity, suppressAccountsWithoutContacts,
                        externalSystemName, perAccountLimitedContacts, useContactsPerAccountLimit);
                log.info("Executing GenerateLaunchArtifactsJob with config: " + JsonUtils.serialize(config));
                SparkJobResult result = executeSparkJob(GenerateLaunchArtifactsJob.class, config);
                log.info("GenerateLaunchArtifactsJob Results: " + JsonUtils.serialize(result));
                return result;
            } finally {
                stopSparkSQLSession();
            }
        });
    }

    @VisibleForTesting
    void processSparkJobResults(AudienceType audienceType, CDLExternalSystemName sysName,
            SparkJobResult sparkJobResult) {
        GenerateLaunchArtifactsStepConfiguration config = getConfiguration();
        long accountsAdded = 0L;
        long accountsDeleted = 0L;
        long fullContacts = 0L;
        long contactsAdded = 0L;
        long contactsDeleted = 0L;
        long accumulativeAccounts = getLongValueFromContext(PREVIOUS_ACCUMULATIVE_ACCOUNTS) != null
                ? getLongValueFromContext(PREVIOUS_ACCUMULATIVE_ACCOUNTS)
                : 0L;
        long accumulativeContacts = getLongValueFromContext(PREVIOUS_ACCUMULATIVE_CONTACTS) != null
                ? getLongValueFromContext(PREVIOUS_ACCUMULATIVE_CONTACTS)
                : 0L;
        HdfsDataUnit addedAccountsDataUnit = sparkJobResult.getTargets().get(0);
        if (addedAccountsDataUnit != null && addedAccountsDataUnit.getCount() > 0) {
            accountsAdded = addedAccountsDataUnit.getCount();
            processHDFSDataUnit(String.format("AddedAccounts_%s", config.getExecutionId()), addedAccountsDataUnit,
                    AudienceType.ACCOUNTS.getInterfaceName(),
                    getAddDeltaTableContextKeyByAudienceType(AudienceType.ACCOUNTS));
        } else {
            log.info(String.format("No new Added %ss", AudienceType.ACCOUNTS.asBusinessEntity().name()));
        }
        HdfsDataUnit removedAccountsDataUnit = sparkJobResult.getTargets().get(1);
        if (removedAccountsDataUnit != null && removedAccountsDataUnit.getCount() > 0) {
            accountsDeleted = removedAccountsDataUnit.getCount();
            processHDFSDataUnit(String.format("RemovedAccounts_%s", config.getExecutionId()), removedAccountsDataUnit,
                    AudienceType.ACCOUNTS.getInterfaceName(),
                    getRemoveDeltaTableContextKeyByAudienceType(AudienceType.ACCOUNTS));
        } else {
            log.info(String.format("No Removed %ss", AudienceType.ACCOUNTS.asBusinessEntity().name()));
        }
        HdfsDataUnit fullContactsDataUnit = sparkJobResult.getTargets().get(2);
        if (fullContactsDataUnit != null && fullContactsDataUnit.getCount() > 0) {
            fullContacts = fullContactsDataUnit.getCount();
            processHDFSDataUnit(String.format("AccountsWithFullContacts_%s", config.getExecutionId()),
                    fullContactsDataUnit, AudienceType.ACCOUNTS.getInterfaceName(), ADDED_ACCOUNTS_FULL_CONTACTS_TABLE);
        } else {
            log.info("No Full contacts");
        }
        if (audienceType == AudienceType.CONTACTS) {
            HdfsDataUnit addedContactsDataUnit = sparkJobResult.getTargets().get(3);
            if (addedContactsDataUnit != null && addedContactsDataUnit.getCount() > 0) {
                contactsAdded = addedContactsDataUnit.getCount();
                if (sysName == CDLExternalSystemName.Outreach) {
                    if (contactsAdded > outreachMaxLaunch) {
                        throw new LedpException(LedpCode.LEDP_40097,
                                new String[] { outreachMaxLaunch.toString(), sysName.toString() });
                    }
                }
                processHDFSDataUnit(String.format("AddedContacts_%s", config.getExecutionId()), addedContactsDataUnit,
                        audienceType.getInterfaceName(), getAddDeltaTableContextKeyByAudienceType(audienceType));
            } else {
                log.info(String.format("No new Added %ss", audienceType.asBusinessEntity().name()));
            }

            HdfsDataUnit removedContactsDataUnit = sparkJobResult.getTargets().get(4);
            if (removedContactsDataUnit != null && removedContactsDataUnit.getCount() > 0) {
                contactsDeleted = removedContactsDataUnit.getCount();
                processHDFSDataUnit(String.format("RemovedContacts_%s", config.getExecutionId()),
                        removedContactsDataUnit, audienceType.getInterfaceName(),
                        getRemoveDeltaTableContextKeyByAudienceType(audienceType));
            } else {
                log.info(String.format("No new Removed %ss", audienceType.asBusinessEntity().name()));
            }
        }
        /*
         * PLS-15540 Accumulative Launched = Add - Delete + Previous
         * Accumulative Launched Suppressed = Selected - Accumulative Launched
         * Incremental Launched = Add + Delete
         */
        accumulativeAccounts += accountsAdded - accountsDeleted;
        accumulativeContacts += contactsAdded - contactsDeleted;
        log.info(String.format("accountsAdded=%d, accountsDeleted=%d, accumulativeAccounts=%d", accountsAdded,
                accountsDeleted, accumulativeAccounts));
        log.info(String.format("contactsAdded=%d, contactsDeleted=%d, fullContacts=%d, accumulativeContacts=%d",
                contactsAdded, contactsDeleted, fullContacts, accumulativeContacts));
        putLongValueInContext(ACCOUNTS_ADDED, accountsAdded);
        putLongValueInContext(ACCOUNTS_DELETED, accountsDeleted);
        putLongValueInContext(ACCUMULATIVE_ACCOUNTS, accumulativeAccounts);
        putLongValueInContext(CONTACTS_ADDED, contactsAdded);
        putLongValueInContext(CONTACTS_DELETED, contactsDeleted);
        putLongValueInContext(FULL_CONTACTS, fullContacts);
        putLongValueInContext(ACCUMULATIVE_CONTACTS, accumulativeContacts);
    }

    private void processHDFSDataUnit(String tableName, HdfsDataUnit dataUnit, String primaryKey, String contextKey) {
        log.info(getHDFSDataUnitLogEntry(tableName, dataUnit));
        Table dataUnitTable = toTable(tableName, primaryKey, dataUnit);
        metadataProxy.createTable(customerSpace.getTenantId(), dataUnitTable.getName(), dataUnitTable);
        putObjectInContext(contextKey, tableName);
        log.info("Created " + tableName + " at " + dataUnitTable.getExtracts().get(0).getPath());
    }

    private Set<Lookup> buildLookupsByEntity(BusinessEntity mainEntity, List<ColumnMetadata> fieldMappingMetadata, PlayLaunchChannel playLaunchChannel) {
        Set<String> entityLookups = getBaseLookupFieldsByEntity(mainEntity, playLaunchChannel);
        return mergeWithExportFields(mainEntity, entityLookups, fieldMappingMetadata);
    }

    private Set<Lookup> addRatingLookups(RatingEngine model, Set<Lookup> accountLookups) {
        if (model != null && StringUtils.isNotBlank(model.getId()) && model.getPublishedIteration() != null) {
            accountLookups.add(new AttributeLookup(BusinessEntity.Rating, model.getId()));

            if (model.getType() != RatingEngineType.RULE_BASED) {
                accountLookups.add(new AttributeLookup(BusinessEntity.Rating, model.getId() + "_score"));
            }
            if (model.getType() == RatingEngineType.CROSS_SELL
                    && ((AIModel) model.getPublishedIteration()).getPredictionType() == PredictionType.EXPECTED_VALUE) {
                accountLookups.add(new AttributeLookup(BusinessEntity.Rating, model.getId() + "_ev"));
            }
            return accountLookups;
        }
        return accountLookups;
    }

    private Set<Lookup> mergeWithExportFields(BusinessEntity mainEntity, Set<String> entityFields,
                                              List<ColumnMetadata> fieldMappingMetadata) {
        if (CollectionUtils.isNotEmpty(fieldMappingMetadata)) {
            List<ColumnMetadata> unExportableFields = fieldMappingMetadata.stream()
                    .filter(cm -> !BusinessEntity.EXPORT_ACCOUNT_ENTITIES.contains(cm.getEntity())
                            && cm.getEntity() != BusinessEntity.Contact)
                    .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(unExportableFields)) {
                log.warn("Unexportable columns found in columns received from FieldMetadata Service, "
                        + "following fields will be skipped!");
                log.warn(Arrays.toString(unExportableFields.stream().map(ColumnMetadata::getAttrName).toArray()));
            }
            Set<Lookup> mergedLookups = fieldMappingMetadata.stream()
                    .filter(cm -> BusinessEntity.EXPORT_ACCOUNT_ENTITIES.contains(cm.getEntity())
                            || cm.getEntity() == BusinessEntity.Contact) //
                    .filter(cm -> !cm.isCampaignDerivedField()) //
                    .filter(cm -> isValidEntity(cm.getEntity(), mainEntity)) //
                    .filter(cm -> !entityFields.contains(cm.getAttrName())) //
                    .map(cm -> new AttributeLookup(cm.getEntity(), cm.getAttrName())) //
                    .collect(Collectors.toSet());
            entityFields.forEach(f -> mergedLookups.add(new AttributeLookup(mainEntity, f)));
            return mergedLookups;
        } else {
            return entityFields.stream().map(f -> new AttributeLookup(mainEntity, f)).collect(Collectors.toSet());
        }
    }

    private boolean isValidEntity(BusinessEntity fieldEntity, BusinessEntity mainEntity) {
        if (mainEntity == BusinessEntity.Account) {
            return fieldEntity != BusinessEntity.Contact;
        }
        if (mainEntity == BusinessEntity.Contact) {
            return fieldEntity == BusinessEntity.Contact;
        }
        return false;
    }

    private Set<String> getBaseLookupFieldsByEntity(BusinessEntity entity, PlayLaunchChannel playLaunchChannel) {
        switch (entity) {
        case Account:
            return new HashSet<>(Arrays.asList(InterfaceName.AccountId.name(), //
                    InterfaceName.CompanyName.name(), //
                    InterfaceName.LDC_Name.name()));
        case Contact:
            Set<String> set = new HashSet<>();
            set.addAll(Arrays.asList(InterfaceName.AccountId.name(), //
                    InterfaceName.ContactId.name(), //
                    InterfaceName.CompanyName.name(), //
                    InterfaceName.Email.name(), //
                    InterfaceName.ContactName.name(), //
                    InterfaceName.City.name(), //
                    InterfaceName.State.name(), //
                    InterfaceName.Country.name(), //
                    InterfaceName.PostalCode.name(), //
                    InterfaceName.PhoneNumber.name(), //
                    InterfaceName.Title.name(), //
                    InterfaceName.Address_Street_1.name()));
            if (!baseOnOtherTapType) {
                /*
                 * PLS-16386 Add FirstName and LastName
                 */
                String attributeSetName = getAttributeSetName(playLaunchChannel);
                CustomerSpace cs = configuration.getCustomerSpace();
                log.info("Trying to get the attrsUsage with {} for tenant {}.", attributeSetName, cs.getTenantId());
                Map<String, Boolean> map = servingStoreProxy.getAttrsUsage(cs.getTenantId(), BusinessEntity.Contact,
                        Predefined.Enrichment, attributeSetName, additionalContactAttr, null);
                log.info("attrsUsage for firstName & lastName=" + map);
                map.keySet().stream().filter(key -> map.get(key)).forEach(key -> set.add(key));
                log.info("set=" + set);
            }
            return set;
        default:
            throw new LedpException(LedpCode.LEDP_32001,
                    new String[] { String.format("Entity %s not supported", entity.name()) });
        }
    }

    private String getAttributeSetName(PlayLaunchChannel playLaunchChannel) {
        String attributeSetName = AttributeUtils.DEFAULT_ATTRIBUTE_SET_NAME;
        if (CDLExternalSystemName.AWS_S3.equals(playLaunchChannel.getLookupIdMap().getExternalSystemName())) {
            S3ChannelConfig s3ChannelConfig = (S3ChannelConfig) playLaunchChannel.getChannelConfig();
            attributeSetName = s3ChannelConfig.getAttributeSetName();
        }
        return attributeSetName;
    }

    @VisibleForTesting
    boolean shouldSkipStep(AudienceType audienceType, CDLExternalSystemName externalSystemName) {
        Map<String, Long> counts = getMapObjectFromContext(DELTA_TABLE_COUNTS, String.class, Long.class);
        log.info("Counts: " + JsonUtils.serialize(counts));
        switch (externalSystemName) {
        case Salesforce:
        case Eloqua:
            return MapUtils.isEmpty(counts) || //
                    (counts.getOrDefault(getAddDeltaTableContextKeyByAudienceType(audienceType), 0L) <= 0L);
        case Adobe_Audience_Mgr:
        case MediaMath:
        case TradeDesk:
        case Verizon_Media:
        case Google_Display_N_Video_360:
        case AppNexus:
        case AWS_S3:
        case Marketo:
        case Facebook:
        case LinkedIn:
        case Outreach:
        case GoogleAds:
            return MapUtils.isEmpty(counts) || (counts.getOrDefault(getAddDeltaTableContextKeyByAudienceType(audienceType), 0L) <= 0L && //
                            counts.getOrDefault(getRemoveDeltaTableContextKeyByAudienceType(audienceType), 0L) <= 0L);
        default:
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Channel of type " + externalSystemName + " not yet supported" });
        }
    }

    @Override
    protected CustomerSpace parseCustomerSpace(GenerateLaunchArtifactsStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected DataCollection.Version parseDataCollectionVersion(
            GenerateLaunchArtifactsStepConfiguration stepConfiguration) {
        if (version == null) {
            version = configuration.getVersion();
        }
        return version;
    }

    @Override
    protected String parseEvaluationDateStr(GenerateLaunchArtifactsStepConfiguration stepConfiguration) {
        if (StringUtils.isBlank(evaluationDate)) {
            evaluationDate = periodProxy.getEvaluationDate(parseCustomerSpace(stepConfiguration).toString());
        }
        return evaluationDate;
    }

    @Override
    protected AttributeRepository parseAttrRepo(GenerateLaunchArtifactsStepConfiguration stepConfiguration) {
        AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
        if (attrRepo == null) {
            throw new RuntimeException("Cannot find attribute repo in context");
        }
        return attrRepo;
    }

    private String getHDFSDataUnitLogEntry(String tag, HdfsDataUnit dataUnit) {
        if (dataUnit == null) {
            return tag + " data set empty";
        }
        return tag + ", " + JsonUtils.serialize(dataUnit);
    }

    private String getAddDeltaTableContextKeyByAudienceType(AudienceType audienceType) {
        switch (audienceType) {
        case ACCOUNTS:
            return ADDED_ACCOUNTS_DELTA_TABLE;
        case CONTACTS:
            return ADDED_CONTACTS_DELTA_TABLE;
        default:
            return null;
        }
    }

    private String getRemoveDeltaTableContextKeyByAudienceType(AudienceType audienceType) {
        switch (audienceType) {
        case ACCOUNTS:
            return REMOVED_ACCOUNTS_DELTA_TABLE;
        case CONTACTS:
            return REMOVED_CONTACTS_DELTA_TABLE;
        default:
            return null;
        }
    }
}
