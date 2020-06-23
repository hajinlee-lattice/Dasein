package com.latticeengines.cdl.workflow.steps.rebuild;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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

import com.latticeengines.cdl.workflow.steps.BaseProcessAnalyzeSparkStep;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ChangeListConfig;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.spark.exposed.job.cdl.MergeChangeListJob;
import com.latticeengines.spark.exposed.job.common.CopyJob;

@Component(UpdateAccountFeatures.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateAccountFeatures extends BaseProcessAnalyzeSparkStep<ProcessAccountStepConfiguration> {
    static final String BEAN_NAME = "updateAccountFeatures";

    private static final Logger log = LoggerFactory.getLogger(UpdateAccountFeatures.class);

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    private boolean shortCutMode = false;
    private Table latticeAccountTable;
    private Table oldAccountFeaturesTable;
    private Table newAccountFeaturesTable;
    private Table fullChangelistTable;
    private List<ColumnMetadata> featureSchema;
    private String joinKey;

    @Override
    public void execute() {
        bootstrap();
        Table tableInCtx = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_FEATURE_TABLE_NAME);
        shortCutMode = (tableInCtx != null);
        if (shortCutMode) {
            log.info("Found AccountFeatures table in context, go through short-cut mode.");
            dataCollectionProxy.upsertTable(customerSpace.toString(), tableInCtx.getName(), //
                    TableRoleInCollection.AccountFeatures, inactive);
        } else {
            preExecution();
            if (oldAccountFeaturesTable == null) {
                // Old table doesn't exist, use LatticeAccount table to generate AccountFeatures
                if (latticeAccountTable != null) {
                    log.info("LatticeAccount table name is {}, path is {} ", latticeAccountTable.getName(),
                            latticeAccountTable.getExtracts().get(0).getPath());
                    HdfsDataUnit input = latticeAccountTable.toHdfsDataUnit("LatticeAccount");
                    HdfsDataUnit features = select(input);

                    postExecution();
                } else {
                    throw new RuntimeException("Even LatticeAccount table doesn't exist, can't continue");
                }
            } else {
                // Old table exists, apply AccountFeatures related changelist into existing table
                log.info("Existing AccountFeatures table name is {}, path is {} ",
                        oldAccountFeaturesTable.getName(), oldAccountFeaturesTable.getExtracts().get(0).getPath());
                HdfsDataUnit input = oldAccountFeaturesTable.toHdfsDataUnit("OldAccountFeatures");
                HdfsDataUnit applied = applyChangelist(input);

                postExecution();
            }
        }
    }

    private void preExecution() {
        latticeAccountTable = attemptGetTableRole(TableRoleInCollection.LatticeAccount, false);
        fullChangelistTable = getTableSummaryFromKey(customerSpace.toString(), FULL_CHANGELIST_TABLE_NAME);
        oldAccountFeaturesTable = attemptGetTableRole(TableRoleInCollection.AccountFeatures, false);
        if (fullChangelistTable == null) {
            throw new RuntimeException("Full changelist doesn't exist, can't continue");
        }
        joinKey = (configuration.isEntityMatchEnabled() && !inMigrationMode()) ? InterfaceName.EntityId.name()
                : InterfaceName.AccountId.name();
        log.info("joinKey {}", joinKey);
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

    private HdfsDataUnit select(HdfsDataUnit input) {
        log.info("UpdateAccountFeatures, select step");
        CopyConfig config = getFeaturesCopyConfig();
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(input);
        config.setInput(inputs);
        SparkJobResult result = runSparkJob(CopyJob.class, config);
        HdfsDataUnit output = result.getTargets().get(0);

        // Upsert AccountFeatures table
        String tableName = NamingUtils.timestamp("AccountFeatures");
        newAccountFeaturesTable = toTable(tableName, output);

        return output;
    }

    private CopyConfig getFeaturesCopyConfig() {
        featureSchema = servingStoreProxy //
                .getAllowedModelingAttrs(customerSpace.toString(), true, inactive) //
                .collectList().block();
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

        CopyConfig config = new CopyConfig();
        config.setSelectAttrs(retainAttrNames);
        return config;
    }

    private HdfsDataUnit applyChangelist(HdfsDataUnit input) {
        log.info("UpdateAccountFeatures, apply changelist step");
        ChangeListConfig config = new ChangeListConfig();
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(input);
        config.setInput(inputs);
        config.setJoinKey(joinKey);
        SparkJobResult result = runSparkJob(MergeChangeListJob.class, config);
        HdfsDataUnit output = result.getTargets().get(0);

        // Upsert AccountFeature table
        String tableName = NamingUtils.timestamp("AccountFeatures");
        newAccountFeaturesTable = toTable(tableName, output);

        return output;
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
