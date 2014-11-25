package com.latticeengines.skald.model;

import java.util.Map;

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
}
