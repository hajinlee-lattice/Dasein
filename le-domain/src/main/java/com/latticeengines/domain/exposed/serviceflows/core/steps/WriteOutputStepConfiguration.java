package com.latticeengines.domain.exposed.serviceflows.core.steps;

import java.util.HashMap;
import java.util.Map;

public class WriteOutputStepConfiguration extends MicroserviceStepConfiguration {
    private Map<String, String> output = new HashMap<>();

    public Map<String, String> getOutput() {
        return output;
    }

    public void setOutput(Map<String, String> output) {
        this.output = output;
    }

    public void putOutput(String key, String value) {
        output.put(key, value);
    }
}
