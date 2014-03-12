package com.latticeengines.dataplatform.exposed.domain;

import org.codehaus.jackson.annotate.JsonIgnore;

public class Job implements HasId<String> {

    private String id;
    private Model model;
    
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
    
    @Override
    public String toString() {
        return id;
    }

}
