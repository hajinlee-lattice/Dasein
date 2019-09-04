package com.latticeengines.domain.exposed.spark;


import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class SparkScriptOutput {

    @JsonProperty("OutputStr")
    private String outputStr;

    @JsonProperty("Output")
    private List<HdfsDataUnit> output;

    public String getOutputStr() {
        return outputStr;
    }

    public void setOutputStr(String outputStr) {
        this.outputStr = outputStr;
    }

    public List<HdfsDataUnit> getOutput() {
        return output;
    }

    public void setOutput(List<HdfsDataUnit> output) {
        this.output = output;
    }
}
