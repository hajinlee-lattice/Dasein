package com.latticeengines.cdl.workflow.steps.update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.spark.exposed.job.common.UpsertJob;

public abstract class BaseMergeTableRoleDiff<T extends BaseProcessEntityStepConfiguration> extends RunSparkJob<T, UpsertConfig, UpsertJob> {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    private Table masterTable;
    private Table diffTable;

    String mergedTableName;

    protected abstract TableRoleInCollection getTableRole();

    protected boolean saveParquet() {
        return false;
    }

    @Override
    protected Class<UpsertJob> getJobClz() {
        return UpsertJob.class;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(T stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

    @Override
    protected UpsertConfig configureJob(T stepConfiguration) {
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), getTableRole(), active);
        if (masterTable == null || CollectionUtils.isEmpty(masterTable.getExtracts())) {
            throw new RuntimeException("There is no " + getTableRole() + " master table! " +
                    "Please rebuild the serving store first.");
        }
        log.info("Set masterTable=" + masterTable.getName());
        diffTable = getDiffTable();
        if (diffTable == null) {
            throw new RuntimeException(
                    "Failed to find diff " + getTableRole() + " table in customer " + customerSpace);
        }
        UpsertConfig jobConfig = UpsertConfig.joinBy(getJoinKey());
        HdfsDataUnit input1 = SparkUtils.tableToHdfsUnit(masterTable, "Master", yarnConfiguration);
        HdfsDataUnit input2 = diffTable.toHdfsDataUnit("Diff");
        jobConfig.setInput(Arrays.asList(input1, input2));
        if (saveParquet()) {
            jobConfig.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
        }
        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String tenantId = CustomerSpace.shortenCustomerSpace(parseCustomerSpace(configuration).toString());
        mergedTableName = NamingUtils.timestamp(getTableRole().name());
        Table mergedTable = toTable(mergedTableName, getTableRole().getPrimaryKey().name(), result.getTargets().get(0));
        overlayMetadata(mergedTable);

        if (publishToRedshift()) {
            mergedTableName = NamingUtils.timestamp(tenantId + "_" + getTableRole().name());
            mergedTable.setName(mergedTableName);
        }

        metadataProxy.createTable(tenantId, mergedTableName, mergedTable);
        DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        dataCollectionProxy.upsertTable(tenantId, mergedTableName, getTableRole(), inactive);

        if (publishToRedshift()) {
            exportTableRoleToRedshift(mergedTableName);
        }
    }

    private void overlayMetadata(Table resultTable) {
        List<Attribute> masterAttrs = masterTable.getAttributes().stream() //
                .peek(attr -> attr.setPid(null)) //
                .collect(Collectors.toList());
        resultTable.setAttributes(masterAttrs);
    }

    private Table getDiffTable() {
        Map<TableRoleInCollection, String> diffTableNames = getMapObjectFromContext(PROCESSED_DIFF_TABLES,
                TableRoleInCollection.class, String.class);
        String diffTableName = diffTableNames.get(getTableRole());
        return metadataProxy.getTable(customerSpace.toString(), diffTableName);
    }

    protected boolean publishToRedshift() {
        return true;
    }

    private void exportTableRoleToRedshift(String tableName) {
        TableRoleInCollection tableRole = getTableRole();
        String distKey = tableRole.getPrimaryKey().name();
        List<String> sortKeys = new ArrayList<>(tableRole.getForeignKeysAsStringList());
        if (!sortKeys.contains(tableRole.getPrimaryKey().name())) {
            sortKeys.add(tableRole.getPrimaryKey().name());
        }
        String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
        RedshiftExportConfig config = new RedshiftExportConfig();
        config.setTableName(tableName);
        config.setDistKey(distKey);
        config.setSortKeys(sortKeys);
        config.setInputPath(inputPath + "/*.avro");
        config.setUpdateMode(false);
        addToListInContext(TABLES_GOING_TO_REDSHIFT, config, RedshiftExportConfig.class);
    }

    protected String getJoinKey() {
        return getTableRole().getPrimaryKey().name();
    }

}

