package com.latticeengines.datafabric.connector.generic;

import java.util.HashMap;
import java.util.Map;

public class ProcessorAdapterFactory {

    private static Map<String, ProcessorAdapter> storeAdapterMap = new HashMap<>();
    private static Map<String, Class<? extends ProcessorAdapter>> storeAdapterClassMap = new HashMap<>();
    static {
        storeAdapterClassMap.put("HDFS", HDFSProcessorAdapter.class);
        storeAdapterClassMap.put("DYNAMO", DynamoProcessorAdapter.class);

    }

    public static ProcessorAdapter getAdapter(String store, GenericSinkConnectorConfig connectorConfig) {
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
