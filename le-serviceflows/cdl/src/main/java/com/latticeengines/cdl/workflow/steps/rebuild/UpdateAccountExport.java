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
import com.latticeengines.domain.exposed.spark.cdl.ChangeListConfig;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.spark.exposed.job.cdl.MergeChangeListJob;
import com.latticeengines.spark.exposed.job.common.CopyJob;

@Component(UpdateAccountExport.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateAccountExport extends BaseProcessAnalyzeSparkStep<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "updateAccountExport";

    private static final Logger log = LoggerFactory.getLogger(UpdateAccountExport.class);

    @Inject
    private ServingStoreProxy servingStoreProxy;

    private boolean shortCutMode = false;
    private Table fullAccountTable;
    private Table oldAccountExportTable;
    private Table newAccountExportTable;
    private Table fullChangelistTable;
    private List<ColumnMetadata> exportSchema;
    private String joinKey;

    @Override
    public void execute() {
        bootstrap();
        Table tableInCtx = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_EXPORT_TABLE_NAME);
        shortCutMode = (tableInCtx != null);
        if (shortCutMode) {
            log.info("Found AccountExport table in context, go through short-cut mode.");
            dataCollectionProxy.upsertTable(customerSpace.toString(), tableInCtx.getName(), //
                    TableRoleInCollection.AccountExport, inactive);
        } else {
            preExecution();

            if (shouldRebuild()) {
                log.info("UpdateAccountExport, rebuild AccountExport table");

                if (fullAccountTable != null) {
                    log.info("FullAccount table name is {}, path is {} ", fullAccountTable.getName(),
                            fullAccountTable.getExtracts().get(0).getPath());
                    HdfsDataUnit fullAccount = fullAccountTable.toHdfsDataUnit("FullAccount");
                    // Select from the full account table
                    select(fullAccount);
                } else {
                    throw new RuntimeException("Full account table doesn't exist, can't rebuild");
                }
            } else {
                // apply changelists to generate new AccountExport table
                if (fullChangelistTable != null) {
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
        joinKey = (configuration.isEntityMatchEnabled() && !inMigrationMode()) ? InterfaceName.EntityId.name()
                : InterfaceName.AccountId.name();
        log.info("joinKey {}", joinKey);
        fullAccountTable = getTableSummaryFromKey(customerSpace.toString(), FULL_ACCOUNT_TABLE_NAME);
        oldAccountExportTable = attemptGetTableRole(TableRoleInCollection.AccountExport, false);
        fullChangelistTable = getTableSummaryFromKey(customerSpace.toString(), FULL_CHANGELIST_TABLE_NAME);
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
        return (oldAccountExportTable == null)
                || getObjectFromContext(REBUILD_LATTICE_ACCOUNT, Boolean.class);
    }

    private void select(HdfsDataUnit input) {
        log.info("UpdateAccountExport, select step");

        CopyConfig config = getExportCopyConfig();
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(input);
        config.setInput(inputs);
        SparkJobResult result = runSparkJob(CopyJob.class, config);
        HdfsDataUnit output = result.getTargets().get(0);

        // Upsert AccountExport table
        String tableName = NamingUtils.timestamp("AccountExport");
        newAccountExportTable = toTable(tableName, output);
    }

    private CopyConfig getExportCopyConfig() {
        exportSchema = servingStoreProxy
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

        CopyConfig config = new CopyConfig();
        config.setSelectAttrs(retainAttrNames);
        return config;
    }

    private void applyChangelist(HdfsDataUnit input) {
        log.info("UpdateAccountExport, apply changelist step");
        ChangeListConfig config = new ChangeListConfig();
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(toDataUnit(fullChangelistTable, "FullChangelist")); // changelist
        inputs.add(input); // source table
        config.setInput(inputs);
        config.setJoinKey(joinKey);
        SparkJobResult result = runSparkJob(MergeChangeListJob.class, config);
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
