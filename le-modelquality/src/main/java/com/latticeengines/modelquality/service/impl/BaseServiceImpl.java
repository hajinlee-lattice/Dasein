package com.latticeengines.modelquality.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public class BaseServiceImpl implements InitializingBean {

    @Autowired
    private VersionManager versionManager;

    @Value("${modelquality.file.upload.hdfs.dir}")
    private String hdfsDir;

    @Value("${common.pls.url}")
    private String plsApiHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    protected Map<String, String> getActiveStack() {
        return internalResourceRestApiProxy.getActiveStack();
    }

    protected String getVersion() {
        Map<String, String> stackInfo;
        try {
            stackInfo = getActiveStack();
        } catch (Exception e) {
            stackInfo = new HashMap<>();
            stackInfo.put("CurrentStack", "");
        }
        String stackName = stackInfo.get("CurrentStack");
        String version = versionManager.getCurrentVersionInStack(stackName).replace('/', '_');
        return version;
    }

    protected String getHdfsDir() {
        return hdfsDir;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(plsApiHostPort);
    }

}
