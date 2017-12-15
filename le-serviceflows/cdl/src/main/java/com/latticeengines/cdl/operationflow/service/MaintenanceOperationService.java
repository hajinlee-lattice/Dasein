package com.latticeengines.cdl.operationflow.service;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;

public abstract class MaintenanceOperationService<T extends MaintenanceOperationConfiguration> {

    private static Map<Class<? extends MaintenanceOperationConfiguration>,
            MaintenanceOperationService<? extends MaintenanceOperationConfiguration>> map = new HashMap<>();

    @SuppressWarnings("unchecked")
    public MaintenanceOperationService() {
        map.put((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], this);
    }

    public static MaintenanceOperationService<? extends MaintenanceOperationConfiguration> getMaintenanceService
            (Class<? extends MaintenanceOperationConfiguration> clz) {
        return map.get(clz);
    }

    public abstract void invoke(T config);


}
