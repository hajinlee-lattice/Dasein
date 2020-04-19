package com.latticeengines.hadoop.service.impl;


import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
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

    @Value("${hadoop.leds.version.p2}")
    private String ledsP2Version;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${dataplatform.default.python.version}")
    private String pythonVersion;

    @Override
    public String getLedsVersion() {
        return getLedsVersion(pythonVersion);
    }

    @Override
    public String getLedsVersion(String pythonVersion) {
        if (StringUtils.isBlank(pythonVersion)) {
            pythonVersion = this.pythonVersion;
        }
        if ("3".equals(pythonVersion)) {
            return ledsVersion;
        } else {
            return ledsP2Version;
        }
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
