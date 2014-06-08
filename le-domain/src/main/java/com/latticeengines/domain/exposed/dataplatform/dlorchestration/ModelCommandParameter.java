package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import com.latticeengines.domain.exposed.dataplatform.HasId;


public class ModelCommandParameter implements HasId<Integer> {

    private Integer commandId;
    private String key;
    private String value;

    public ModelCommandParameter(Integer commandId, String key, String value) {
        super();
        this.commandId = commandId;
        this.key = key;
        this.value = value;
    }

    @Override
    public Integer getId() {
        return commandId;
    }

    @Override
    public void setId(Integer id) {
        this.commandId = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
