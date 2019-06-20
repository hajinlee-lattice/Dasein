package com.latticeengines.metadata.service;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public class DataUnitRuntimeServiceRegistry {

    private static final Logger log = LoggerFactory.getLogger(DataUnitRuntimeServiceRegistry.class);

    private static Map<Class<? extends DataUnit>, DataUnitRuntimeService> registry = new HashMap<>();

    public static <T extends DataUnit> void register(Class<T> unitClz, DataUnitRuntimeService service) {
        registry.put(unitClz, service);
        log.info("Registered a runtime service of type " + service.getClass().getSimpleName() //
                + " for data units of type " + unitClz.getSimpleName());
    }

    public static <T extends DataUnit> DataUnitRuntimeService getRunTimeService(Class<T> unitClz) {
        return registry.get(unitClz);
    }

}
