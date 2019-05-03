package com.latticeengines.domain.exposed.spark.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class RepartitionConfig extends SparkJobConfig {

    public static final String NAME = "repartition";

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    // required
    @JsonProperty("Partitions")
    private int partitions;

    // optional
    @JsonProperty("PartitionKeys")
    private List<String> partitionKeys;

    public static RepartitionConfig of(int partitions, String... partitionKeys) {
        RepartitionConfig config = new RepartitionConfig();
        List<String> keyList = new ArrayList<>();
        for (String key: partitionKeys) {
            if (StringUtils.isNotBlank(key)) {
                keyList.add(key);
            }
        }
        if (!keyList.isEmpty()) {
            config.setPartitionKeys(keyList);
        }
        config.setPartitions(partitions);
        return config;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public static String getNAME() {
        return NAME;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }
}
