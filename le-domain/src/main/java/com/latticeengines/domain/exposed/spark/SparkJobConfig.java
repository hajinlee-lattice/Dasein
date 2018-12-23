package com.latticeengines.domain.exposed.spark;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.WorkflowSparkJobConfig;

import reactor.core.publisher.Flux;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "Name")
@JsonSubTypes({ @JsonSubTypes.Type(value = ScriptJobConfig.class, name = ScriptJobConfig.NAME),
        @JsonSubTypes.Type(value = TestJoinJobConfig.class, name = TestJoinJobConfig.NAME),
        @JsonSubTypes.Type(value = WorkflowSparkJobConfig.class, name = "WorkflowSparkJobConfig"), })
public abstract class SparkJobConfig {

    @JsonProperty("Input")
    private List<DataUnit> input;

    @JsonProperty("Workspace")
    private String workspace;

    public abstract String getName();

    public int getNumTargets() {
        return 1;
    }

    public String getWorkspace() {
        return workspace;
    }

    public void setWorkspace(String workspace) {
        this.workspace = workspace;
    }

    public List<DataUnit> getInput() {
        return input;
    }

    public void setInput(List<DataUnit> input) {
        this.input = input;
    }

    @JsonIgnore
    public List<HdfsDataUnit> getTargets() {
        String root;
        if (workspace.endsWith("/")) {
            root = workspace.substring(0, workspace.lastIndexOf("/"));
        } else {
            root = workspace;
        }
        return Flux.range(1, getNumTargets()).map(idx -> //
        HdfsDataUnit.fromPath(root + "/Output" + idx) //
        ).collectList().block();
    }

}
