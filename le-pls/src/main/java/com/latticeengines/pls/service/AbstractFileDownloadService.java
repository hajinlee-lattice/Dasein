package com.latticeengines.pls.service;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.pls.FileDownloadConfig;

public abstract class AbstractFileDownloadService<T extends FileDownloadConfig> {

    private static Map<Class<? extends FileDownloadConfig>,
            AbstractFileDownloadService<? extends FileDownloadConfig>> map =
            new HashMap<>();

    @SuppressWarnings("unchecked")
    public AbstractFileDownloadService() {
        map.put((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], this);
    }

    public abstract void downloadByConfig(T config, HttpServletRequest request, HttpServletResponse response)
            throws Exception;

    public static AbstractFileDownloadService<? extends FileDownloadConfig> getDownloadService(
            Class<? extends FileDownloadConfig> clz) {
        return map.get(clz);
    }
}
