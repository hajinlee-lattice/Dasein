package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountFeatures;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.cdl.workflow.steps.BaseProcessAnalyzeSparkStep;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.JoinAccountStoresConfig;
import com.latticeengines.domain.exposed.spark.common.ApplyChangeListConfig;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.spark.exposed.job.cdl.JoinAccountStores;
import com.latticeengines.spark.exposed.job.common.ApplyChangeListJob;
import com.latticeengines.spark.exposed.job.common.CopyJob;

@Lazy
@Component(UpdateAccountFeatures.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateAccountFeatures extends BaseProcessAnalyzeSparkStep<ProcessAccountStepConfiguration> {
    static final String BEAN_NAME = "updateAccountFeatures";

    private static final Logger log = LoggerFactory.getLogger(UpdateAccountFeatures.class);

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    private Table oldAccountFeaturesTable;
    private Table newAccountFeaturesTable;
    private Table customerAccountChangelistTable;
    private Table latticeAccountChangelistTable;
    private List<ColumnMetadata> featureSchema;
    private String joinKey;

    @Override
    public void execute() {
        bootstrap();
        Table tableInCtx = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_FEATURE_TABLE_NAME);
        boolean shortCutMode = (tableInCtx != null);
        if (shortCutMode) {
            log.info("Found AccountFeatures table in context, go through short-cut mode.");
            dataCollectionProxy.upsertTable(customerSpace.toString(), tableInCtx.getName(), //
                    TableRoleInCollection.AccountFeatures, inactive);
        } else if (shouldDoNothing()) {
            linkInactiveTable(AccountFeatures);
        } else {
            preExecution();

            if (shouldRebuild()) {
                log.info("UpdateAccountFeatures, rebuild AccountFeatures table");
                Table customerAccount = attemptGetTableRole(ConsolidatedAccount, true);
                Table latticeAccount = attemptGetTableRole(LatticeAccount, false);
                HdfsDataUnit output;
                if (latticeAccount == null) {
                    log.warn("This tenant does not have Lattice Account.");
                    // Select from the customer account table
                    output = select(customerAccount.toHdfsDataUnit("Account"));
                } else {
                    output = join( //
                            customerAccount.toHdfsDataUnit("CustomerAccount"), //
                            latticeAccount.toHdfsDataUnit("LatticeAccount") //
                    );
                }
                String tableName = NamingUtils.timestamp("AccountFeatures");
                newAccountFeaturesTable = toTable(tableName, output);
            } else {
                // Apply changelists to generate new AccountFeatures table
                if (customerAccountChangelistTable != null || latticeAccountChangelistTable != null) {
                    log.info("Existing AccountFeatures table name is {}, path is {} ", oldAccountFeaturesTable.getName(),
                            oldAccountFeaturesTable.getExtracts().get(0).getPath());
                    HdfsDataUnit input = oldAccountFeaturesTable.toHdfsDataUnit("OldAccountFeatures");
                    applyChangelist(input);
                } else {
                    throw new RuntimeException("Full changelist doesn't exist, can't continue");
                }
            }

            postExecution();
        }
    }

    private void preExecution() {
        joinKey = InterfaceName.AccountId.name();
        log.info("joinKey {}", joinKey);
        customerAccountChangelistTable = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_CHANGELIST_TABLE_NAME);
        latticeAccountChangelistTable = getTableSummaryFromKey(customerSpace.toString(), LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME);
        oldAccountFeaturesTable = attemptGetTableRole(TableRoleInCollection.AccountFeatures, false);
        featureSchema = getFeatureSchema();
    }

    protected void postExecution() {
        if (newAccountFeaturesTable != null) {
            setAccountFeatureTableSchema(newAccountFeaturesTable);
            metadataProxy.createTable(customerSpace.toString(), newAccountFeaturesTable.getName(),
                    newAccountFeaturesTable);
            dataCollectionProxy.upsertTable(customerSpace.toString(), newAccountFeaturesTable.getName(),
                    TableRoleInCollection.AccountFeatures, inactive);
            exportToS3AndAddToContext(newAccountFeaturesTable, ACCOUNT_FEATURE_TABLE_NAME);
        }
    }

    private boolean shouldRebuild() {
//        return (oldAccountFeaturesTable == null) ||
//                Boolean.TRUE.equals(getObjectFromContext(REBUILD_LATTICE_ACCOUNT, Boolean.class));
        // always rebuild due to an issue in ApplyChangeList spark job
        return true;
    }

    private boolean shouldDoNothing() {
        boolean customerAccountHasChanged = isChanged(ConsolidatedAccount);
        boolean latticeAccountHasChanged = isChanged(LatticeAccount);
        boolean shouldDoNothing = !(customerAccountHasChanged || latticeAccountHasChanged);
        log.info("customerAccountChanged={}, latticeAccountChanged={}, shouldDoNothing={}",
                customerAccountHasChanged, latticeAccountHasChanged, shouldDoNothing);
        return shouldDoNothing;
    }

    private HdfsDataUnit select(HdfsDataUnit input) {
        List<String> retainAttrs = featureSchema.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        CopyConfig config = new CopyConfig();
        config.setSelectAttrs(retainAttrs);
        config.setInput(Collections.singletonList(input));
        SparkJobResult result = runSparkJob(CopyJob.class, config);
        return result.getTargets().get(0);
    }

    private HdfsDataUnit join(HdfsDataUnit customerInput, HdfsDataUnit latticeInput) {
        List<String> retainAttrs = featureSchema.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        JoinAccountStoresConfig config = new JoinAccountStoresConfig();
        config.setRetainAttrs(retainAttrs);
        config.setInput(Arrays.asList(customerInput, latticeInput));
        SparkJobResult result = runSparkJob(JoinAccountStores.class, config);
        return result.getTargets().get(0);
    }

    private List<ColumnMetadata> getFeatureSchema() {
        List<ColumnMetadata> featureSchema = servingStoreProxy //
                .getAllowedModelingAttrs(customerSpace.toString(), true, inactive) //
                .collectList().block();
        Preconditions.checkNotNull(featureSchema);
        List<String> retainAttrNames = featureSchema.stream().map(ColumnMetadata::getAttrName) //
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(retainAttrNames)) {
            retainAttrNames = new ArrayList<>();
        }
        log.info("retainAttrNames from servingStore: {}", CollectionUtils.size(retainAttrNames));
        if (!retainAttrNames.contains(InterfaceName.AccountId.name())) {
            retainAttrNames.add(InterfaceName.AccountId.name());
        }
        if (!retainAttrNames.contains(InterfaceName.LatticeAccountId.name())) {
            retainAttrNames.add(InterfaceName.LatticeAccountId.name());
        }
        return featureSchema;
    }

    private void applyChangelist(HdfsDataUnit input) {
        log.info("UpdateAccountFeatures, apply changelist step");
        List<String> retainAttrs = featureSchema.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        ApplyChangeListConfig config = new ApplyChangeListConfig();
        config.setInput(Arrays.asList( //
                input, // source table
                toDataUnit(customerAccountChangelistTable, "Changelist1"), // changelist of customer account
                toDataUnit(latticeAccountChangelistTable, "Changelist2") // changelist of lattice account
        ));
        config.setHasSourceTbl(true);
        config.setJoinKey(joinKey);
        config.setIncludeAttrs(retainAttrs);
        SparkJobResult result = runSparkJob(ApplyChangeListJob.class, config);
        HdfsDataUnit output = result.getTargets().get(0);

        // Upsert AccountFeature table
        String tableName = NamingUtils.timestamp("AccountFeatures");
        newAccountFeaturesTable = toTable(tableName, output);
    }

    private void setAccountFeatureTableSchema(Table table) {
        overwriteServingSchema(table, featureSchema);
        String dataCloudVersion = configuration.getDataCloudVersion();
        List<ColumnMetadata> amCols = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Model,
                dataCloudVersion);
        Map<String, ColumnMetadata> amColMap = new HashMap<>();
        amCols.forEach(cm -> amColMap.put(cm.getAttrName(), cm));
        List<Attribute> attrs = new ArrayList<>();
        table.getAttributes().forEach(attr0 -> {
            if (amColMap.containsKey(attr0.getName())) {
                ColumnMetadata cm = amColMap.get(attr0.getName());
                if (Category.ACCOUNT_ATTRIBUTES.equals(cm.getCategory())) {
                    attr0.setTags(Tag.INTERNAL);
                }
            }
            attrs.add(attr0);
        });
        table.setAttributes(attrs);
    }

    private void overwriteServingSchema(Table table, List<ColumnMetadata> schema) {
        Map<String, ColumnMetadata> colMap = new HashMap<>();
        schema.forEach(cm -> colMap.put(cm.getAttrName(), cm));
        List<Attribute> attrs = new ArrayList<>();
        table.getAttributes().forEach(attr0 -> {
            if (colMap.containsKey(attr0.getName())) {
                ColumnMetadata cm = colMap.get(attr0.getName());
                if (cm.getFundamentalType() != null) {
                    attr0.setFundamentalType(cm.getFundamentalType());
                }
                if (cm.getLogicalDataType() != null) {
                    attr0.setLogicalDataType(cm.getLogicalDataType());
                }
            }
            attrs.add(attr0);
        });
        table.setAttributes(attrs);
    }
}
