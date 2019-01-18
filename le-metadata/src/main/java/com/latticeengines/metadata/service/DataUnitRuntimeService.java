package com.latticeengines.metadata.service;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public abstract class DataUnitRuntimeService<T extends DataUnit> {
    private static final Logger log = LoggerFactory.getLogger(DataUnitRuntimeService.class);

    private static Map<Class<? extends DataUnit>, DataUnitRuntimeService<? extends DataUnit>> map = new HashMap<>();

    @SuppressWarnings("unchecked")
    public DataUnitRuntimeService() {
        map.put((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], this);
    }

    public static DataUnitRuntimeService<? extends DataUnit> getRunTimeService(
            Class<? extends DataUnit> dataUnit) {
        return map.get(dataUnit);
    }
    public abstract Boolean delete(T dataUnit);

    public abstract Boolean renameTableName(T dataUnit, String tablename);
}
