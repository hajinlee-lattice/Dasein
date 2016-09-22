package com.latticeengines.domain.exposed.datacloud.transformation;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.match.MatchKey;

public class ExternalEnrichRequest {

    private String avroInputGlob;
    private String avroOuptutDir;
    private Map<MatchKey, List<String>> inputKeyMapping;
    private Map<MatchKey, String> outputKeyMapping;
    private String recordName;

    public String getAvroInputGlob() {
        return avroInputGlob;
    }

    public void setAvroInputGlob(String avroInputGlob) {
        this.avroInputGlob = avroInputGlob;
    }

    public String getAvroOuptutDir() {
        return avroOuptutDir;
    }

    public void setAvroOuptutDir(String avroOuptutDir) {
        this.avroOuptutDir = avroOuptutDir;
    }

    public Map<MatchKey, List<String>> getInputKeyMapping() {
        return inputKeyMapping;
    }

    public void setInputKeyMapping(Map<MatchKey, List<String>> inputKeyMapping) {
        this.inputKeyMapping = inputKeyMapping;
    }

    public Map<MatchKey, String> getOutputKeyMapping() {
        return outputKeyMapping;
    }

    public void setOutputKeyMapping(Map<MatchKey, String> outputKeyMapping) {
        this.outputKeyMapping = outputKeyMapping;
    }

    public String getRecordName() {
        return recordName;
    }

    public void setRecordName(String recordName) {
        this.recordName = recordName;
    }
}
