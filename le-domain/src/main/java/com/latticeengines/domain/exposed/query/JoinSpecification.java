package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

/**
 * Specification of a join within the context of a Query. We can use a secondary
 * object either via a lookup or via an exists clause.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class JoinSpecification {
    @JsonProperty("source_type")
    private SchemaInterpretation sourceType;

    @JsonProperty("destination_type")
    private SchemaInterpretation destinationType;

    @JsonProperty("destination_object_usage")
    private ObjectUsage destinationObjectUsage;

    public JoinSpecification(SchemaInterpretation sourceType, SchemaInterpretation destinationType,
            ObjectUsage destinationObjectUsage) {
        this.sourceType = sourceType;
        this.destinationType = destinationType;
        this.destinationObjectUsage = destinationObjectUsage;
    }

    public SchemaInterpretation getSourceType() {
        return sourceType;
    }

    public void setSourceType(SchemaInterpretation sourceType) {
        this.sourceType = sourceType;
    }

    public SchemaInterpretation getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(SchemaInterpretation destinationType) {
        this.destinationType = destinationType;
    }

    public ObjectUsage getDestinationObjectUsage() {
        return destinationObjectUsage;
    }

    public void setDestinationObjectUsage(ObjectUsage destinationObjectUsage) {
        this.destinationObjectUsage = destinationObjectUsage;
    }
}
