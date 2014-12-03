package com.latticeengines.domain.exposed.skald.model;

import java.util.Map;

import com.latticeengines.domain.exposed.skald.model.FieldType;

public class TransformDefinition {
    public TransformDefinition(String name, String output, FieldType type, Map<String, Object> arguments) {
        this.name = name;
        this.output = output;
        this.type = type;
        this.arguments = arguments;
    }

    // Serialization constructor.
    public TransformDefinition() {
    }

    // The name of the transform file to be invoked.
    public String name;

    // The name of the field this transform will populate.
    public String output;

    // The output type of the transformation.
    public FieldType type;

    // Any arguments to the transformation function.
    public Map<String, Object> arguments;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((arguments == null) ? 0 : arguments.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((output == null) ? 0 : output.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
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
        TransformDefinition other = (TransformDefinition) obj;
        if (arguments == null) {
            if (other.arguments != null)
                return false;
        } else if (!arguments.equals(other.arguments))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (output == null) {
            if (other.output != null)
                return false;
        } else if (!output.equals(other.output))
            return false;
        if (type != other.type)
            return false;
        return true;
    }
}
