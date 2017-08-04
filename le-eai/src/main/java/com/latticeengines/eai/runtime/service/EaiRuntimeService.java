package com.latticeengines.eai.runtime.service;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;

public abstract class EaiRuntimeService<T extends EaiJobConfiguration> {

    private static Map<Class<? extends EaiJobConfiguration>, EaiRuntimeService<? extends EaiJobConfiguration>> map = new HashMap<>();

    protected Function<Float, Void> progressReporter;

    @SuppressWarnings("unchecked")
    public EaiRuntimeService() {
        map.put((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], this);
    }

    public static EaiRuntimeService<? extends EaiJobConfiguration> getRunTimeService(
            Class<? extends EaiJobConfiguration> clz) {
        return map.get(clz);
    }

    //public abstract void initailize(T config);

    public abstract void invoke(T config);

    //public abstract void finalize(T config);

    public void setProgressReporter(Function<Float, Void> progressReporter) {
        this.progressReporter = progressReporter;
    }

    protected void setProgress(float progress) {
        progressReporter.apply(progress);
    }

}
