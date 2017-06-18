package com.latticeengines.domain.exposed.query;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

import edu.emory.mathcs.backport.java.util.Collections;

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

    @JsonProperty("src_entity")
    private BusinessEntity sourceEntity;

    @JsonProperty("dest_entity")
    private BusinessEntity destinationEntity;

    @JsonProperty("dest_obj_usage")
    private ObjectUsage destinationObjectUsage;

    public JoinSpecification(SchemaInterpretation sourceType, SchemaInterpretation destinationType,
            ObjectUsage destinationObjectUsage) {
        this.sourceType = sourceType;
        this.destinationType = destinationType;
        this.destinationObjectUsage = destinationObjectUsage;
    }

    public JoinSpecification(BusinessEntity sourceEntity, BusinessEntity destinationEntity,
                             ObjectUsage destinationObjectUsage) {
        this.sourceEntity = sourceEntity;
        this.destinationEntity = destinationEntity;
        this.destinationObjectUsage = destinationObjectUsage;
    }

    public BusinessEntity getSourceEntity() {
        return sourceEntity;
    }

    public void setSourceEntity(BusinessEntity sourceEntity) {
        this.sourceEntity = sourceEntity;
    }

    public BusinessEntity getDestinationEntity() {
        return destinationEntity;
    }

    public void setDestinationEntity(BusinessEntity destinationEntity) {
        this.destinationEntity = destinationEntity;
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

    @Override
    public boolean equals(Object object) {
        if (object == null || !(object instanceof JoinSpecification)) {
            return false;
        } else {
            JoinSpecification join = (JoinSpecification) object;
            return this.comprableForm().equals(join.comprableForm());
        }
    }

    @Override
    public int hashCode() {
        return this.comprableForm().hashCode();
    }

    private String comprableForm() {
        List<String> names = Arrays.asList(sourceEntity.name(), destinationEntity.name(), destinationObjectUsage.name());
        Collections.sort(names);
        return StringUtils.join(names, "-");
    }
}
