package com.latticeengines.cdl.workflow.steps;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSet;
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
import com.latticeengines.domain.exposed.spark.common.ApplyChangeListConfig;
import com.latticeengines.domain.exposed.spark.common.ChangeListConfig;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.spark.exposed.job.common.ApplyChangeListJob;
import com.latticeengines.spark.exposed.job.common.CreateChangeListJob;

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

        // deal with account/contact batch store whose change list is generated in validate
        generateSpecialChangeTable(accountDataUnit, version, ConsolidatedAccount, InterfaceName.AccountId.name(),
                ACCOUNT_CHANGELIST_TABLE_NAME);
        generateSpecialChangeTable(contactDataUnit, version, ConsolidatedContact, InterfaceName.ContactId.name(),
                CONTACT_CHANGELIST_TABLE_NAME);

    }

    /**
     * this method generate multiple export config for roles
     **/
    private void generateExportConfig(ElasticSearchDataUnit dataUnit, String version,
                                      List<TableRoleInCollection> publishedRoles, String entityKey) {

        // if data unit is null or active table is null, publish the inactive table directly
        // if data unit is not null and active table is not equal to inactive table, publish the partial table
        for (TableRoleInCollection role : publishedRoles) {
            Table activeTable = dataCollectionProxy.getTable(customerSpace.toString(), role, active);
            if (dataUnit == null || activeTable == null) {
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
                addToListInContext(TABLES_GOING_TO_ES, config, ElasticSearchExportConfig.class);
            } else if (isChanged(role)) {
                log.info("Create Change List for role=" + role + " activeTable=" + activeTable);
                addToListInContext(TABLES_GOING_TO_ES,
                        createChangeListAndChangedTable(role, dataUnit, entityKey),
                        ElasticSearchExportConfig.class);
            }
        }
    }

    /**
     * the method to deal with the ConsolidateAccount and ConsolidateContact
     */
    private void generateSpecialChangeTable(ElasticSearchDataUnit dataUnit, String version,
                                            TableRoleInCollection role, String entityKey, String contextKey) {
        Table activeTable = dataCollectionProxy.getTable(customerSpace.toString(), ConsolidatedAccount, active);
        if (dataUnit == null || activeTable == null) {
            log.info("publish inactive table to elastic search");
            Table inactiveTable = dataCollectionProxy.getTable(customerSpace.toString(), role, inactive);
            if (inactiveTable == null) {
                log.info("found null inactive table for role {} in {}", role, customerSpaceStr);
                return ;
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
            addToListInContext(TABLES_GOING_TO_ES, config, ElasticSearchExportConfig.class);
        } else if (isChanged(role, contextKey)) {
            String tableName = getStringValueFromContext(contextKey);
            if (StringUtils.isNotBlank(tableName)) {
                Table changeListTbl = metadataProxy.getTableSummary(customerSpaceStr, tableName);
                if (changeListTbl != null) {
                    HdfsDataUnit changeListDataUnit = changeListTbl.toHdfsDataUnit(role + "_ChangeList");
                    HdfsDataUnit activeDataUnit = activeTable.toHdfsDataUnit(role + "_ActiveTable");
                    String joinKey = (configuration.isEntityMatchEnabled() && !inMigrationMode()) ?
                            InterfaceName.EntityId.name()
                            : entityKey;
                    addToListInContext(TABLES_GOING_TO_ES,
                            generateChangeTable(activeDataUnit, changeListDataUnit, joinKey, entityKey, dataUnit, role),
                            ElasticSearchExportConfig.class);
                }
            }
        }
    }



    /**
     * two steps : create change list and generate change table
     * @param role
     * @param dataUnit
     * @param entityKey
     * @return
     */
    private ElasticSearchExportConfig createChangeListAndChangedTable(TableRoleInCollection role,
                                                                      ElasticSearchDataUnit dataUnit, String entityKey) {
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

        // in migration mode, need to use AccountId/ContactId because legacy
        // won't have EntityId column
        String joinKey = (configuration.isEntityMatchEnabled() && !inMigrationMode()) ? InterfaceName.EntityId.name()
                : entityKey;

        // step1: generate change list
        ChangeListConfig config = getChangeListConfig(joinKey);
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(inactiveDataUnit);
        inputs.add(activeDataUnit);
        config.setInput(inputs);
        SparkJobResult result = runSparkJob(CreateChangeListJob.class, config);

        String changeListTableName = NamingUtils.timestamp(role.name() + "_ChangeList");
        HdfsDataUnit changeListDataUnit = result.getTargets().get(0);
        Table changeListTable = toTable(changeListTableName, changeListDataUnit);
        metadataProxy.createTable(customerSpaceStr, changeListTableName, changeListTable);
        log.info("Create change list table {} in {}", changeListTableName, customerSpace);

        // step2: use change list and active table to generate partial changed table
        return generateChangeTable(activeDataUnit, changeListDataUnit, joinKey, entityKey, dataUnit, role);
    }


    /**
     * the method to generate partial changed table
     */
    private ElasticSearchExportConfig generateChangeTable(HdfsDataUnit activeDataUnit, HdfsDataUnit changeListDataUnit,
                                                          String joinKey,
                                                          String entityKey, ElasticSearchDataUnit dataUnit,
                                                          TableRoleInCollection role) {
        List<DataUnit> inputs2 = new ArrayList<>();
        inputs2.add(activeDataUnit);
        inputs2.add(changeListDataUnit);
        ApplyChangeListConfig changeTableConfig = getChangeTableConfig(joinKey, entityKey);
        changeTableConfig.setInput(inputs2);
        SparkJobResult result2 = runSparkJob(ApplyChangeListJob.class, changeTableConfig);
        HdfsDataUnit changeTableDataUnit = result2.getTargets().get(0);
        String changeTableName = NamingUtils.timestamp(role.name() + "_ChangeTable");
        Table changeTable = toTable(changeTableName, changeTableDataUnit);
        metadataProxy.createTable(customerSpaceStr, changeTableName, changeTable);
        log.info("create partial updated table {} in {}", changeTableName, customerSpace);
        ElasticSearchExportConfig exportConfig = new ElasticSearchExportConfig();
        exportConfig.setTableName(changeTableName);
        exportConfig.setTableRoleInCollection(role);
        exportConfig.setSignature(dataUnit.getSignature());
        return exportConfig;
    }

    private ChangeListConfig getChangeListConfig(String joinKey) {
        ChangeListConfig config = new ChangeListConfig();
        config.setJoinKey(joinKey);
        config.setExclusionColumns(
                Arrays.asList(InterfaceName.CDLCreatedTime.name(), InterfaceName.CDLUpdatedTime.name(), joinKey));
        return config;
    }

    private ApplyChangeListConfig getChangeTableConfig(String joinKey, String entityKey) {
        ApplyChangeListConfig config = new ApplyChangeListConfig();
        config.setJoinKey(joinKey);
        config.setHasSourceTbl(true);
        config.setSetDeletedToNull(true);
        config.setAttrsForbidToSet(ImmutableSet.of(entityKey));
        return config;
    }


    private List<TableRoleInCollection> getAccountPublishedRoles() {
        return Arrays.asList(
                //AccountLookup,
                //ConsolidatedAccount,
                CalculatedCuratedAccountAttribute,
                CalculatedPurchaseHistory,
                PivotedRating,
                WebVisitProfile,
                OpportunityProfile,
                AccountMarketingActivityProfile,
                CustomIntentProfile);
    }

    private List<TableRoleInCollection> getContactPublishedRoles() {
        //ConsolidatedContact
        return Collections.singletonList(CalculatedCuratedContact);
    }
}
