package com.latticeengines.modelquality.service.impl;

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
    
    @Value("${modelquality.pls.api.hostport}")
    private String plsApiHostPort;
    
    private InternalResourceRestApiProxy internalResourceRestApiProxy;
    
    protected Map<String, String> getActiveStack() {
        return internalResourceRestApiProxy.getActiveStack();
    }
    
    protected String getVersion() {
        Map<String, String> stackInfo = getActiveStack();
        String stackName = stackInfo.get("CurrentStack");
        return versionManager.getCurrentVersionInStack(stackName);
    }
    
    protected String getHdfsDir() {
        return hdfsDir;
    }
    
    @Override
    public void afterPropertiesSet() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(plsApiHostPort);
    }

}
