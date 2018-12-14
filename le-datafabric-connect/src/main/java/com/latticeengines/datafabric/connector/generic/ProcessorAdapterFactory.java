package com.latticeengines.datafabric.connector.generic;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;

public class ProcessorAdapterFactory {

    private static Map<FabricStoreEnum, ProcessorAdapter> storeAdapterMap = new HashMap<>();
    private static Map<FabricStoreEnum, Class<? extends ProcessorAdapter>> storeAdapterClassMap = new HashMap<>();
    static {
        storeAdapterClassMap.put(FabricStoreEnum.HDFS, HDFSProcessorAdapter.class);
        storeAdapterClassMap.put(FabricStoreEnum.S3, S3ProcessorAdapter.class);
        storeAdapterClassMap.put(FabricStoreEnum.DYNAMO, DynamoProcessorAdapter.class);
    }

    public static ProcessorAdapter getAdapter(FabricStoreEnum store, GenericSinkConnectorConfig connectorConfig) {
        try {
            ProcessorAdapter adapter = storeAdapterMap.get(store);
            if (adapter == null) {
                synchronized (storeAdapterMap) {
                    if (adapter == null) {
                        Class<? extends ProcessorAdapter> adapterClass = storeAdapterClassMap.get(store);
                        adapter = adapterClass.newInstance();
                        adapter.setup(connectorConfig);
                        storeAdapterMap.put(store, adapter);
                    }
                }
            }
            return adapter;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

}
