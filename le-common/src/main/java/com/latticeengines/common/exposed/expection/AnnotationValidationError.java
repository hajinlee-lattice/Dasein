package com.latticeengines.common.exposed.expection;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AnnotationValidationError {

    @JsonProperty("FieldName")
    private String fieldName;
    @JsonProperty("AnnotationName")
    private String annotationName;

    public AnnotationValidationError(String fieldName, String annotationName) {
        this.fieldName = fieldName;
        this.annotationName = annotationName;
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public String getAnnotationName() {
        return this.annotationName;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 89 * hash + (this.fieldName != null ? this.fieldName.hashCode() : 0);
        hash = 89 * hash + (this.annotationName != null ? this.annotationName.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || object.getClass() != this.getClass()) {
            return false;
        }
        AnnotationValidationError error = (AnnotationValidationError) object;
        if (this.getAnnotationName().equals(error.getAnnotationName())
                && this.getFieldName().equals(error.getFieldName())) {
            return true;
        }
        return false;
    }
}
