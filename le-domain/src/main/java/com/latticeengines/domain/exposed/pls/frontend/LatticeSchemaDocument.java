package com.latticeengines.domain.exposed.pls.frontend;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class LatticeSchemaDocument {

    @JsonProperty
    private Map<SchemaInterpretation, List<LatticeSchemaField>> schemaToLatticeSchemaFields;

    public void setSchemaToLatticeSchemaFields(Map<SchemaInterpretation, List<LatticeSchemaField>> schemaToLatticeSchemaFields) {
        this.schemaToLatticeSchemaFields = schemaToLatticeSchemaFields;
    }

    public  Map<SchemaInterpretation, List<LatticeSchemaField>> getSchemaToLatticeSchemaFields() {
        return this.schemaToLatticeSchemaFields;
    }
}
