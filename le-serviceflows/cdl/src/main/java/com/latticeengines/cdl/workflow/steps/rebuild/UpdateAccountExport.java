package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountExport;
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

import com.latticeengines.cdl.workflow.steps.BaseProcessAnalyzeSparkStep;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.JoinAccountStoresConfig;
import com.latticeengines.domain.exposed.spark.common.ApplyChangeListConfig;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.spark.exposed.job.cdl.JoinAccountStores;
import com.latticeengines.spark.exposed.job.common.ApplyChangeListJob;
import com.latticeengines.spark.exposed.job.common.CopyJob;

@Lazy
@Component(UpdateAccountExport.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateAccountExport extends BaseProcessAnalyzeSparkStep<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "updateAccountExport";

    private static final Logger log = LoggerFactory.getLogger(UpdateAccountExport.class);

    @Inject
    private ServingStoreProxy servingStoreProxy;

    private Table oldAccountExportTable;
    private Table newAccountExportTable;
    private Table customerAccountChangelistTable;
    private Table latticeAccountChangelistTable;
    private List<ColumnMetadata> exportSchema;
    private String joinKey;

    @Override
    public void execute() {
        bootstrap();
        Table tableInCtx = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_EXPORT_TABLE_NAME);
        boolean shortCutMode = (tableInCtx != null);
        if (shortCutMode) {
            log.info("Found AccountExport table in context, go through short-cut mode.");
            dataCollectionProxy.upsertTable(customerSpace.toString(), tableInCtx.getName(), //
                    TableRoleInCollection.AccountExport, inactive);
        } else if (shouldDoNothing()) {
            linkInactiveTable(AccountExport);
        } else {
            preExecution();

            if (shouldRebuild()) {
                log.info("UpdateAccountExport, rebuild AccountExport table");
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
                String tableName = NamingUtils.timestamp("AccountExport");
                newAccountExportTable = toTable(tableName, output);
            } else {
                // apply changelists to generate new AccountExport table
                if (customerAccountChangelistTable != null || latticeAccountChangelistTable != null) {
                    log.info("Existing AccountExport table name is {}, path is {} ", oldAccountExportTable.getName(),
                            oldAccountExportTable.getExtracts().get(0).getPath());
                    HdfsDataUnit input = oldAccountExportTable.toHdfsDataUnit("OldAccountExport");
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
        oldAccountExportTable = attemptGetTableRole(TableRoleInCollection.AccountExport, false);
        customerAccountChangelistTable = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_CHANGELIST_TABLE_NAME);
        latticeAccountChangelistTable = getTableSummaryFromKey(customerSpace.toString(), LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME);
        exportSchema = getExportSchema();
    }

    protected void postExecution() {
        if (newAccountExportTable != null) {
            setAccountExportTableSchema(newAccountExportTable);
            metadataProxy.createTable(customerSpace.toString(), newAccountExportTable.getName(), newAccountExportTable);
            dataCollectionProxy.upsertTable(customerSpace.toString(), newAccountExportTable.getName(),
                    TableRoleInCollection.AccountExport, inactive);
            exportToS3AndAddToContext(newAccountExportTable, ACCOUNT_EXPORT_TABLE_NAME);
        }
    }

    private boolean shouldRebuild() {
//        return (oldAccountExportTable == null) ||
//                Boolean.TRUE.equals(getObjectFromContext(REBUILD_LATTICE_ACCOUNT, Boolean.class));
        // always rebuild due to an issue in ApplyChangeList spark job
        return true;
    }

    private boolean shouldDoNothing() {
        boolean customerAccountHasChanged = isChanged(ConsolidatedAccount, ACCOUNT_CHANGELIST_TABLE_NAME);
        boolean latticeAccountHasChanged = isChanged(LatticeAccount, LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME);
        boolean shouldDoNothing = !(customerAccountHasChanged || latticeAccountHasChanged);
        log.info("customerAccountChanged={}, latticeAccountChanged={}, shouldDoNothing={}",
                customerAccountHasChanged, latticeAccountHasChanged, shouldDoNothing);
        return shouldDoNothing;
    }

    private HdfsDataUnit select(HdfsDataUnit input) {
        List<String> retainAttrs = exportSchema.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        CopyConfig config = new CopyConfig();
        config.setSelectAttrs(retainAttrs);
        config.setInput(Collections.singletonList(input));
        SparkJobResult result = runSparkJob(CopyJob.class, config);
        return result.getTargets().get(0);
    }

    private HdfsDataUnit join(HdfsDataUnit customerInput, HdfsDataUnit latticeInput) {
        List<String> retainAttrs = exportSchema.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        JoinAccountStoresConfig config = new JoinAccountStoresConfig();
        config.setRetainAttrs(retainAttrs);
        config.setInput(Arrays.asList(customerInput, latticeInput));
        config.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
        setPartitionMultiplier(4);
        SparkJobResult result = runSparkJob(JoinAccountStores.class, config);
        setPartitionMultiplier(1);
        return result.getTargets().get(0);
    }

    private List<ColumnMetadata> getExportSchema() {
        List<ColumnMetadata> exportSchema = servingStoreProxy
                .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account, null, inactive) //
                .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState())) //
                .filter(cm -> !(Boolean.FALSE.equals(cm.getCanSegment()) //
                        && Boolean.FALSE.equals(cm.getCanEnrich()))) //
                .collectList().block();
        List<String> retainAttrNames = exportSchema.stream() //
                .map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(retainAttrNames)) {
            retainAttrNames = new ArrayList<>();
        }
        if (!retainAttrNames.contains(InterfaceName.AccountId.name())) {
            retainAttrNames.add(InterfaceName.AccountId.name());
        }
        return exportSchema;
    }

    private void applyChangelist(HdfsDataUnit input) {
        log.info("UpdateAccountExport, apply changelist step");
        List<String> retainAttrs = exportSchema.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
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
        // Upsert AccountExport table
        String tableName = NamingUtils.timestamp("AccountExport");
        newAccountExportTable = toTable(tableName, output);
    }

    private void setAccountExportTableSchema(Table table) {
        overwriteServingSchema(table, exportSchema);
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
