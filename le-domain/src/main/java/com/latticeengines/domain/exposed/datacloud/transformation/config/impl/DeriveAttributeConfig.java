package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DeriveAttributeConfig extends TblDrivenTransformerConfig {

    public static final String SUM = "SUM";

    public static final String BOOL_TYPE = "Bool";
    public static final String LONG_TYPE = "Long";
    public static final String DOUBLE_TYPE = "Double";

    static public class DeriveFunc extends TblDrivenFuncConfig implements Serializable {

        /**
         *
         */
        private static final long serialVersionUID = 1L;

        @JsonProperty("Type")
        String type;

        @JsonProperty("Calculation")
        String calculation;

        @JsonProperty("attributes")
        List<String> attributes;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getCalculation() {
            return calculation;
        }

        public void setCalculation(String calculation) {
            this.calculation = calculation;
        }

        public List<String> getAttributes() {
            return attributes;
        }

        public void setAttributes(List<String> attributes) {
            this.attributes = attributes;
        }

        @JsonIgnore
        public Class<?> getTypeClass() {
            if (BOOL_TYPE.equalsIgnoreCase(type)) {
                return Boolean.class;
            } else if (LONG_TYPE.equalsIgnoreCase(type)) {
                return Long.class;
            } else if (DOUBLE_TYPE.equalsIgnoreCase(type)) {
                return Double.class;
            } else {
                return String.class;
            }
        }
    }
}
