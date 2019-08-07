package com.latticeengines.cdl.workflow.service;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}
