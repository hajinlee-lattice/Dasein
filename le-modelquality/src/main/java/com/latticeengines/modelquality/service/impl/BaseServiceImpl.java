package com.latticeengines.modelquality.service.impl;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.hadoop.exposed.service.ManifestService;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public class BaseServiceImpl implements InitializingBean {

    @Inject
    private ManifestService manifestService;

    @Value("${modelquality.file.upload.hdfs.dir}")
    private String hdfsDir;

    @Value("${common.pls.url}")
    private String plsApiHostPort;

    protected InternalResourceRestApiProxy internalResourceRestApiProxy;

    protected Map<String, String> getActiveStack() {
        return internalResourceRestApiProxy.getActiveStack();
    }

    protected String getLedsVersion() {
//        Map<String, String> stackInfo;
//        try {
//            stackInfo = getActiveStack();
//        } catch (Exception e) {
//            stackInfo = new HashMap<>();
//            stackInfo.put("CurrentStack", "");
//        }
//        String stackName = stackInfo.get("CurrentStack");
//        String version = versionManager.getCurrentVersionInStack(stackName);
//        return version;
        return manifestService.getLedsVersion();
    }

    protected String getHdfsDir() {
        return hdfsDir;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(plsApiHostPort);
    }

}
