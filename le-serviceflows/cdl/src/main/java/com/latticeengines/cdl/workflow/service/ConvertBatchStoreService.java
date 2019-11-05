package com.latticeengines.cdl.workflow.service;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.BaseConvertBatchStoreServiceConfiguration;

public abstract class ConvertBatchStoreService<T extends BaseConvertBatchStoreServiceConfiguration> {

    private static final String TARGET_PREFIX = "Converted%sImports";

    private static Map<Class<? extends BaseConvertBatchStoreServiceConfiguration>,
                ConvertBatchStoreService<? extends BaseConvertBatchStoreServiceConfiguration>> map = new HashMap<>();

    @SuppressWarnings("unchecked")
    public ConvertBatchStoreService() {
        map.put((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], this);
    }

    public static ConvertBatchStoreService<? extends BaseConvertBatchStoreServiceConfiguration> getConvertService(
            Class<? extends BaseConvertBatchStoreServiceConfiguration> clz) {
        return map.get(clz);
    }

    /**
     *
     * @param customerSpace identify tenant.
     * @param config convert service configuration
     * @return dataFeedTaskId that store the template and registered data table.
     */
    public abstract String getOutputDataFeedTaskId(String customerSpace, T config);

    /**
     *
     * @param customerSpace identify tenant.
     * @param config convert service configuration
     * @return Total import counts.
     */
    public abstract Long getImportCounts(String customerSpace, T config);

    /**
     *
     * @param customerSpace identify tenant.
     * @param config convert service configuration
     * @return registered data table names (convert to import needs to register table to dataFeedTask.
     */
    public abstract List<String> getRegisteredDataTables(String customerSpace, T config);

    /**
     *
     * @param customerSpace identify tenant.
     * @param config convert service configuration
     * @return Table prefix for transformation dataFlow.
     */
    public String getTargetTablePrefix(String customerSpace, T config) {
        BusinessEntity entity = config.getEntity();
        switch (entity) {
            case Account:
            case Contact:
            case Transaction:
                return String.format(TARGET_PREFIX, entity.name());
            default:
                throw new IllegalArgumentException("Import migration workflow does not support entity: " + entity.name());
        }
    }

    protected Long getTableDataLines(Table table, Configuration yarnConfiguration) {
        if (table == null || table.getExtracts() == null) {
            return 0L;
        }
        Long lines = 0L;
        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            if (!extract.getPath().endsWith("avro")) {
                paths.add(extract.getPath() + "/*.avro");
            } else {
                paths.add(extract.getPath());
            }
        }
        for (String path : paths) {
            lines += AvroUtils.count(yarnConfiguration, path);
        }
        return lines;
    }

    /**
     *
     * @param customerSpace identify tenant.
     * @param config convert service configuration
     * @return Batch store
     */
    public TableRoleInCollection getBatchStore(String customerSpace, T config) {
        BusinessEntity entity = config.getEntity();
        switch (entity) {
            case Account:
            case Contact:
                return entity.getBatchStore();
            case Transaction:
                return TableRoleInCollection.ConsolidatedRawTransaction;
            default:
                throw new IllegalArgumentException("Import migration workflow does not support entity: " + entity.name());
        }
    }

    /**
     *
     * @param customerSpace identify tenant.
     * @param config convert service configuration
     * @return the map for duplication key: sourceColumn, value: destColumn.
     */
    public abstract Map<String, String> getDuplicateMap(String customerSpace, T config);

    /**
     *
     * @param customerSpace identify tenant.
     * @param config convert service configuration
     * @return the map for rename, key: sourceColumn, value: destColumn
     */
    public abstract Map<String, String> getRenameMap(String customerSpace, T config);

    /**
     *
     * @param customerSpace identify tenant.
     * @param config convert service configuration
     * @param importCounts Total converted counts.
     * @param dataTables Data tables that have been registered to dataFeedTask.
     */
    public abstract void updateConvertResult(String customerSpace, T config, Long importCounts,
                                             List<String> dataTables);

    /**
     *
     * @param customerSpace identify tenant.
     * @param config config convert service configuration
     * @param actionId Registered ActionId.
     */
    public abstract void updateRegisteredAction(String customerSpace, T config, Long actionId);

    /**
     *
     * @param migratedImportTableName been converted data table name
     * @param customerSpace identity tenant
     * @param templateTable related entity template
     * @param config config convert service configuration
     * @param yarnConfiguration using to count dataLine
     */
    public abstract void setDataTable(String migratedImportTableName, String customerSpace, Table templateTable,
                             T config, Configuration yarnConfiguration);

    /**
     *
     * @param customerSpace identity tenant
     * @param config config convert service configuration
     * @return templateTable
     */
    public abstract Table verifyTenantStatus(String customerSpace, T config);

    /**
     *
     * @param customerSpace identity tenant
     * @param templateTable tenant templateTable
     * @param masterTable the batchStore table we need convert
     * @param config config convert service configuration
     * @return template Table attribute list
     */
    public abstract List<String> getAttributes(String customerSpace, Table templateTable, Table masterTable, T config);

    /**
     *
     * @param customerSpace identity tenant
     * @param batchStore using to get need convert batchStore table
     * @param config config convert service configuration
     * @return the batchStore table which we need convert
     */
    public abstract Table getMasterTable(String customerSpace, TableRoleInCollection batchStore, T config);
}
