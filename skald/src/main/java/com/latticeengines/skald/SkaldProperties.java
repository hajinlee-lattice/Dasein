package com.latticeengines.skald;

import org.springframework.context.annotation.Configuration;

@Configuration
public class SkaldProperties {
    public String getPod() {
        return getProperty("skald.pod");
    }

    public String getZooKeeperAddress() {
        return getProperty("skald.zookeeper.address");
    }

    public String getHdfsAddress() {
        return getProperty("skald.hdfs.address");
    }

    public String getMatcherAddress() {
        return getProperty("skald.matcher.address");
    }

    public String getHistoryConnection() {
        return getProperty("skald.history.connection");
    }

    private String getProperty(String name) {
        String result = System.getProperty(name);
        if (result == null) {
            throw new RuntimeException("No value specified for property " + name);
        }

        return result;
    }
}
