package com.latticeengines.cdl.operationflow.service;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;

public abstract class MaintenanceOperationService<T extends MaintenanceOperationConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceOperationService.class);

    private static Map<Class<? extends MaintenanceOperationConfiguration>,
            MaintenanceOperationService<? extends MaintenanceOperationConfiguration>> map = new HashMap<>();

    @SuppressWarnings("unchecked")
    public MaintenanceOperationService() {
        map.put((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], this);
    }

    public static MaintenanceOperationService<? extends MaintenanceOperationConfiguration> getMaintenanceService
            (Class<? extends MaintenanceOperationConfiguration> clz) {
        log.info("MaintenanceOperationService Class name: " + clz.getName());
        return map.get(clz);
    }

    public abstract void invoke(T config);


}
