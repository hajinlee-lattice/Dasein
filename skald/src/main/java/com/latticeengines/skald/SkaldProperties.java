package com.latticeengines.skald;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource("file:skald.properties")
public class SkaldProperties {
    public String getPod() {
        return env.getProperty("skald.zookeeper.pod");
    }

    public String getZooKeeperAddress() {
        return env.getProperty("skald.zookeeper.address");
    }

    public String getHdfsAddress() {
        return env.getProperty("skald.hdfs.address");
    }

    @Autowired
    private Environment env;
}
