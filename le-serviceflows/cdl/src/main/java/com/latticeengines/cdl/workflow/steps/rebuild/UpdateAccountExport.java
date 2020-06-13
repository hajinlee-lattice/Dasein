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
    private Table latticeAccountTable;
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
            if (oldAccountExportTable == null) {
                // Old table doesn't exist, use LatticeAccount table to generate AccountExport
                if (latticeAccountTable != null) {
                    log.info("LatticeAccount table name is {}, extract dir is {} ", latticeAccountTable.getName(),
                            latticeAccountTable.getExtracts().get(0).getPath());
                    HdfsDataUnit input = latticeAccountTable.toHdfsDataUnit("LaticeAccount");
                    HdfsDataUnit export = select(input);

                    postExecution();
                } else {
                    throw new RuntimeException("Even LatticeAccount table doesn't exist, can't continue");
                }
            } else {
                // Old table exists, apply AccountExport related changelist into existing table
                log.info("Existing AccountExport table name is {}, path is {} ", oldAccountExportTable.getName(),
                        oldAccountExportTable.getExtracts().get(0).getPath());
                HdfsDataUnit input = oldAccountExportTable.toHdfsDataUnit("OldAccountExport");
                HdfsDataUnit applied = applyChangelist(input);

                postExecution();
            }
        }
    }

    private void preExecution() {
        joinKey = (configuration.isEntityMatchEnabled() && !inMigrationMode()) ? InterfaceName.EntityId.name()
                : InterfaceName.AccountId.name();
        log.info("joinKey {}", joinKey);
        latticeAccountTable = attemptGetTableRole(TableRoleInCollection.LatticeAccount, false);
        oldAccountExportTable = attemptGetTableRole(TableRoleInCollection.AccountExport, false);
        String fullChangelistTableName = getStringValueFromContext(FULL_CHANGELIST_TABLE_NAME);
        log.info("Full changelis table name {}", fullChangelistTableName);
        fullChangelistTable = metadataProxy.getTableSummary(customerSpace.toString(), fullChangelistTableName);
        if (fullChangelistTable == null) {
            throw new RuntimeException("Full changelist doesn't exist, can't continue");
        }
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

    private HdfsDataUnit select(HdfsDataUnit input) {
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

        return output;
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

    private HdfsDataUnit applyChangelist(HdfsDataUnit input) {
        log.info("UpdateAccountExport, apply changelist step");
        ChangeListConfig config = new ChangeListConfig();
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(input);
        config.setInput(inputs);
        config.setJoinKey(joinKey);
        SparkJobResult result = runSparkJob(MergeChangeListJob.class, config);
        HdfsDataUnit output = result.getTargets().get(0);

        // Upsert AccountExport table
        String tableName = NamingUtils.timestamp("AccountExport");
        newAccountExportTable = toTable(tableName, output);

        return output;
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
