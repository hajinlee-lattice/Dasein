package com.latticeengines.domain.exposed.spark;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;

public class SparkJobResult {

    private String output;

    private List<HdfsDataUnit> targets;

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public List<HdfsDataUnit> getTargets() {
        return targets;
    }

    public void setTargets(List<HdfsDataUnit> targets) {
        this.targets = targets;
    }
}
