package com.latticeengines.cdl.workflow.service.impl;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

import com.latticeengines.cdl.workflow.service.ConvertBatchStoreService;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.BaseConvertBatchStoreServiceConfiguration;

public abstract class BaseConvertBatchStoreService<T extends BaseConvertBatchStoreServiceConfiguration> implements ConvertBatchStoreService {

    private static Map<Class<? extends BaseConvertBatchStoreServiceConfiguration>,
                BaseConvertBatchStoreService<? extends BaseConvertBatchStoreServiceConfiguration>> map = new HashMap<>();

    @SuppressWarnings("unchecked")
    public BaseConvertBatchStoreService() {
        map.put((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], this);
    }

    public static BaseConvertBatchStoreService<? extends BaseConvertBatchStoreServiceConfiguration> getConvertService(
            Class<? extends BaseConvertBatchStoreServiceConfiguration> clz) {
        return map.get(clz);
    }
}
