package com.latticeengines.modelquality.service.impl;

import javax.inject.Inject;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.hadoop.exposed.service.ManifestService;

public class BaseServiceImpl implements InitializingBean {

    @Inject
    private ManifestService manifestService;

    @Value("${modelquality.file.upload.hdfs.dir}")
    private String hdfsDir;

    protected String getLedsVersion() {
        return manifestService.getLedsVersion();
    }

    String getHdfsDir() {
        return hdfsDir;
    }

    @Override
    public void afterPropertiesSet() {
    }

}
