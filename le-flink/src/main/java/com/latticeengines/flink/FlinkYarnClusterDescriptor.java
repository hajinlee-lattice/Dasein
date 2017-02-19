package com.latticeengines.flink;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnApplicationMasterRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class FlinkYarnClusterDescriptor extends AbstractYarnClusterDescriptor {

    private Map<String, String> extraJars;

    FlinkYarnClusterDescriptor(YarnConfiguration conf) {
        super(conf);
    }

    @Override
    protected Class<?> getApplicationMasterClass() {
        return YarnApplicationMasterRunner.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setExtraJars(Map<String, String> extraJars) {
        this.extraJars = new HashMap<>(extraJars);
    }

    @Override
    public Map<String, String> getExtraJars() {
        return extraJars;
    }

}
