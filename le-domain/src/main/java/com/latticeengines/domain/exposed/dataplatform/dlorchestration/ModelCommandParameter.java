package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import java.io.Serializable;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Access(AccessType.FIELD)
@IdClass(ModelCommandParameterKey.class)
@Table(name = "LeadScoringCommandParameter")
public class ModelCommandParameter implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Id
    @ManyToOne()
    @JoinColumn(name = "CommandId", nullable = false)
    private ModelCommand modelCommand;
    
    @Id
    @Column(name = "Key", nullable = false)
    private String key;
    
    @Column(name = "Value", length = 65535)
    private String value;
    
    ModelCommandParameter() {
        super();
    }
    
    public ModelCommandParameter(ModelCommand command, String key, String value) {
        super();
        this.modelCommand = command;
        this.key = key;
        this.value = value;
    }
   
    public ModelCommand getModelCommand() {
        return modelCommand;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
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
        ModelCommandParameter other = (ModelCommandParameter) obj;
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
