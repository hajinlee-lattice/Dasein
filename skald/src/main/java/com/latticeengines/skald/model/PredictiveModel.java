package com.latticeengines.skald.model;

import java.util.List;

// TODO Consider splitting the schema and transform elements into their own structure.
public class PredictiveModel {
    // All the fields from the event table that was used to create this model.
    // This schema should match the input provided to the modeling service.
    public List<FieldSchema> fields;

    // The set of transforms to create new fields based on the existing ones.
    // Theses will be executed in order, which should capture any dependencies.
    public List<TransformDefinition> transforms;

    // The PMML model itself in UTF-8 XML form.
    // TODO This needs to either be compressed or stored outside ZooKeeper.
    public String pmml;

    // The name of the output field to use for the positive probability.
    public String output;
}
