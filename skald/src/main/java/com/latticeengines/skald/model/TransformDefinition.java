package com.latticeengines.skald.model;

public class TransformDefinition {
    // The name of the field this transform will populate.
    public String name;

    // The output type of the transformation.
    public FieldType type;

    // The name of the python function that will execute the transformation.
    public String transform;

}
