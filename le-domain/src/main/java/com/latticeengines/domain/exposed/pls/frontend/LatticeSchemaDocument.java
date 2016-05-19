package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

import java.util.Map;
import java.util.List;

public class LatticeSchemaDocument {

    @JsonProperty
    Map<SchemaInterpretation, List<LatticeSchemaField>> schemaToLatticeSchemaFields;

    public void setSchemaToLatticeSchemaFields(Map<SchemaInterpretation, List<LatticeSchemaField>> schemaToLatticeSchemaFields) {
        this.schemaToLatticeSchemaFields = schemaToLatticeSchemaFields;
    }

    public  Map<SchemaInterpretation, List<LatticeSchemaField>> getSchemaToLatticeSchemaFields() {
        return this.schemaToLatticeSchemaFields;
    }
}
