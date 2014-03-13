package com.latticeengines.dataplatform.exposed.domain;

import java.util.Properties;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.latticeengines.dataplatform.util.JsonHelper;

public class Job implements HasId<String> {

    private String id;
    private Model model;
    private Properties appMasterProperties;
    private Properties containerProperties;
    
    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
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

}
