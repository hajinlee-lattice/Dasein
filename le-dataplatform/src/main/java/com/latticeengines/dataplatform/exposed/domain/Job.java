package com.latticeengines.dataplatform.exposed.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.latticeengines.dataplatform.util.JsonHelper;

public class Job implements HasId<String> {

    private String id;
    private String client;
    private Model model;
    private Properties appMasterProperties;
    private Properties containerProperties;
    private String parentJobId;
    private List<String> childJobIds = new ArrayList<String>();
    
    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }
    
    @JsonIgnore
    public ApplicationId getAppId() {
        if (id == null) {
            return null;
        }
        String[] tokens = id.split("_");
        return ApplicationId.newInstance(Long.parseLong(tokens[1]), Integer.parseInt(tokens[2]));
    }

    @JsonIgnore
    public Model getModel() {
        return model;
    }

    @JsonIgnore
    public void setModel(Model model) {
        this.model = model;
    }
    
    public Properties getAppMasterProperties() {
        return appMasterProperties;
    }

    public void setAppMasterProperties(Properties appMasterProperties) {
        this.appMasterProperties = appMasterProperties;
    }

    public Properties getContainerProperties() {
        return containerProperties;
    }

    public void setContainerProperties(Properties containerProperties) {
        this.containerProperties = containerProperties;
    }
    
    @Override
    public String toString() {
        return JsonHelper.serialize(this);
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getParentJobId() {
        return parentJobId;
    }

    public void setParentJobId(String parentJobId) {
        this.parentJobId = parentJobId;
    }

    public List<String> getChildJobIds() {
        return childJobIds;
    }

    public void addChildJobId(String jobId) {
        childJobIds.add(jobId);
    }

}
