package com.latticeengines.hadoop.service.impl;


import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.hadoop.exposed.service.ManifestService;

@Service("manifestService")
public class ManifestServiceImpl implements ManifestService {

    @Inject
    private VersionManager versionManager;

    @Value("${hadoop.leds.version}")
    private String ledsVersion;

    @Value("${common.le.stack}")
    private String leStack;

    @Override
    public String getLedsVersion() {
        return ledsVersion;
    }

    @Override
    public String getLedsPath() {
        return "/datascience/" + getLedsVersion();
    }

    @Override
    public String getLedpStackVersion() {
        return versionManager.getCurrentVersionInStack(leStack);
    }

    @Override
    public String getLedpPath() {
        return "/app/" + getLedpStackVersion();
    }

}
