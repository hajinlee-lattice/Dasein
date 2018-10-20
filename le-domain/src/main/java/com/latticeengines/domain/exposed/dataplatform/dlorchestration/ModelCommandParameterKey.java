package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import java.io.Serializable;

public class ModelCommandParameterKey implements Serializable {

    private static final long serialVersionUID = 1L;
    private ModelCommand modelCommand;
    private String key;

    public ModelCommand getModelCommand() {
        return modelCommand;
    }

    public void setModelCommand(ModelCommand modelCommand) {
        this.modelCommand = modelCommand;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((modelCommand == null) ? 0 : modelCommand.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ModelCommandParameterKey other = (ModelCommandParameterKey) obj;
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        if (modelCommand == null) {
            if (other.modelCommand != null)
                return false;
        } else if (!modelCommand.equals(other.modelCommand))
            return false;
        return true;
    }

}
