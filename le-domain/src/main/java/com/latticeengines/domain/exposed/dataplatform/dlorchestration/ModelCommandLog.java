package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import com.latticeengines.domain.exposed.dataplatform.HasId;

public class ModelCommandLog implements HasId<Integer> {
    
    private int id;
    private int commandId;
    private String message;
    
    @Override
    public Integer getId() {        
        return id;
    }
    
    @Override
    public void setId(Integer id) {
        this.id = id;       
    }

    public int getCommandId() {
        return commandId;
    }

    public void setCommandId(int commandId) {
        this.commandId = commandId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
