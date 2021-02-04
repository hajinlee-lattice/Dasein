package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountLookup;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountMarketingActivityProfile;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.CalculatedCuratedAccountAttribute;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.CalculatedCuratedContact;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.CalculatedPurchaseHistory;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedContact;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.CustomIntentProfile;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.OpportunityProfile;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.PivotedRating;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.WebVisitProfile;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.inject.Inject;

import org.kitesdk.shaded.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.ElasticSearchDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.GenerateChangeTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.GenerateChangeTableConfig;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.spark.exposed.job.common.GenerateChangeTableJob;

@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Component("generateChangeTable")
public class GenerateChangeTable extends BaseProcessAnalyzeSparkStep<GenerateChangeTableConfiguration>{

    private static final Logger log = LoggerFactory.getLogger(GenerateChangeTable.class);

    @Inject
    private BatonService batonService;

    @Inject
    private DataUnitProxy dataUnitProxy;


    @Override
    public void execute() {
        boolean publishToES = batonService.isEnabled(configuration.getCustomerSpace(),
                LatticeFeatureFlag.PUBLISH_TO_ELASTICSEARCH);
        if (!publishToES) {
            log.info("skipping this step as the feature flag is not enabled");
            return ;
        }

        bootstrap();

        String version = ElasticSearchUtils.generateNewVersion();
        // generate export config for account
        ElasticSearchDataUnit accountDataUnit = (ElasticSearchDataUnit) dataUnitProxy.getByNameAndType(customerSpaceStr,
                BusinessEntity.Account.name(), DataUnit.StorageType.ElasticSearch);
        generateExportConfig(accountDataUnit, version, getAccountPublishedRoles(), InterfaceName.AccountId.name());

        // generate export config for for contact
        ElasticSearchDataUnit contactDataUnit = (ElasticSearchDataUnit) dataUnitProxy.getByNameAndType(customerSpaceStr,
                BusinessEntity.Contact.name(), DataUnit.StorageType.ElasticSearch);
        generateExportConfig(contactDataUnit, version, getContactPublishedRoles(), InterfaceName.ContactId.name());


    }

    /**
     * this method generate multiple export config for roles
     **/
    private void generateExportConfig(ElasticSearchDataUnit dataUnit, String version,
                                      List<TableRoleInCollection> publishedRoles, String entityKey) {

        // if data unit is null or active table is null, publish the inactive table directly
        // if data unit is not null and active table is not equal to inactive table, publish the partial table
        // in migration mode, publish the inactive table
        for (TableRoleInCollection role : publishedRoles) {
            Table activeTable = dataCollectionProxy.getTable(customerSpace.toString(), role, active);
            if (dataUnit == null || activeTable == null || inMigrationMode()) {
                log.info("publish inactive table to elastic search");
                Table inactiveTable = dataCollectionProxy.getTable(customerSpace.toString(), role, inactive);
                if (inactiveTable == null) {
                    log.info("found null inactive table for role {} in {}", role, customerSpaceStr);
                    continue;
                }
                ElasticSearchExportConfig config = new ElasticSearchExportConfig();
                if (dataUnit == null) {
                    log.info("data unit is null, use the version {} in {}", version, customerSpaceStr);
                    config.setSignature(version);
                } else {
                    log.info("data unit is not null, use the version in data unit {}", dataUnit.getSignature());
                    config.setSignature(dataUnit.getSignature());
                }
                config.setTableRoleInCollection(role);
                config.setTableName(inactiveTable.getName());
                if (AccountLookup == role) {
                    putObjectInContext(ACCOUNT_LOOKUP_TO_ES, Boolean.TRUE);
                }
                addToListInContext(TABLES_GOING_TO_ES, config, ElasticSearchExportConfig.class);
            } else if (isChanged(role)) {
                log.info("Create Change table for role=" + role + " activeTable=" + activeTable);
                ElasticSearchExportConfig config = createChangedTable(role, dataUnit, version, entityKey);
                if (config != null) {
                    addToListInContext(TABLES_GOING_TO_ES,
                            config,
                            ElasticSearchExportConfig.class);
                }
            }
        }
    }


    /**
     * use spark job to generate change table directly
     * @param role
     * @param dataUnit
     * @param entityKey
     * @return
     */
    private ElasticSearchExportConfig createChangedTable(TableRoleInCollection role,
                                                         ElasticSearchDataUnit dataUnit,
                                                         String version, String entityKey) {
        Table activeTable = dataCollectionProxy.getTable(customerSpaceStr, role, active);
        if (activeTable == null) {
            log.info("There's no active role {} table in {}", role, customerSpaceStr);
            return null;
        }
        Table inactiveTable = dataCollectionProxy.getTable(customerSpaceStr, role, inactive);
        if (inactiveTable == null) {
            log.info("There's no inactive role {} table in {}", role, customerSpaceStr);
            return null;
        }
        HdfsDataUnit activeDataUnit = activeTable.toHdfsDataUnit("Active" + role);
        HdfsDataUnit inactiveDataUnit = inactiveTable.toHdfsDataUnit("Inactive" + role);


        String joinKey;
        if (AccountLookup == role) {
            // account lookup doesn't have entity id
            joinKey = entityKey;
        } else {
            // need to use AccountId/ContactId because legacy
            // won't have EntityId column
            joinKey = configuration.isEntityMatchEnabled() ? InterfaceName.EntityId.name() :
                    entityKey;
        }
        // step1: generate change table
        GenerateChangeTableConfig config = getChangeTableConfig(joinKey, entityKey);
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(activeDataUnit);
        inputs.add(inactiveDataUnit);
        config.setInput(inputs);
        SparkJobResult result = runSparkJob(GenerateChangeTableJob.class, config);

        String changeTableName = NamingUtils.timestamp(role.name() + "_ChangeTable");
        HdfsDataUnit changeTableDataUnit = result.getTargets().get(0);
        if (changeTableDataUnit.getCount() == 0L) {
            log.info("change list is empty for role {} in {}", role, customerSpaceStr);
            return null;
        }
        Table changeListTable = toTable(changeTableName, changeTableDataUnit);
        metadataProxy.createTable(customerSpaceStr, changeTableName, changeListTable);
        log.info("Create change table {} in {}", changeTableName, customerSpace);

        ElasticSearchExportConfig exportConfig = new ElasticSearchExportConfig();
        exportConfig.setTableName(changeTableName);
        exportConfig.setTableRoleInCollection(role);
        if (dataUnit != null) {
            log.info("data unit is not null, use the version in data unit {}", dataUnit.getSignature());
            exportConfig.setSignature(dataUnit.getSignature());
        } else {
            log.info("data unit is null, use the version {} in {}", version, customerSpaceStr);
            exportConfig.setSignature(version);
        }
        if (AccountLookup == role) {
            putObjectInContext(ACCOUNT_LOOKUP_TO_ES, Boolean.TRUE);
        }

        return exportConfig;
    }


    private GenerateChangeTableConfig getChangeTableConfig(String joinKey, String entityKey) {
        GenerateChangeTableConfig config = new GenerateChangeTableConfig();
        config.setJoinKey(joinKey);
        config.setExclusionColumns(
                Arrays.asList(InterfaceName.CDLCreatedTime.name(), InterfaceName.CDLUpdatedTime.name(), joinKey));
        config.setAttrsForbidToSet(ImmutableSet.of(entityKey));
        return config;
    }


    private List<TableRoleInCollection> getAccountPublishedRoles() {
        return Arrays.asList(
                AccountLookup,
                ConsolidatedAccount,
                CalculatedCuratedAccountAttribute,
                CalculatedPurchaseHistory,
                PivotedRating,
                WebVisitProfile,
                OpportunityProfile,
                AccountMarketingActivityProfile,
                CustomIntentProfile);
    }

    private List<TableRoleInCollection> getContactPublishedRoles() {
        return Arrays.asList(
                ConsolidatedContact,
                CalculatedCuratedContact
        );
    }

}
