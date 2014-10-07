package com.latticeengines.sparkdb.operator.impl;

import java.io.Serializable;

import com.latticeengines.domain.exposed.sparkdb.Function;

public class SampleFunctions {

    public static class PassThroughFunction implements Function, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public Object apply(Object... params) {
            if (params != null) {
                return params[0];
            }
            return null;
        }
        
    }

    public static class ConvertBooleanToDouble implements Function, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public Object apply(Object... params) {
            if (params != null) {
                Boolean boolValue = (Boolean) params[0];
                if (boolValue) {
                    return 1.0;
                } else {
                    return 0.0;
                }
            }
            return 0.0;
        }
        
    }

    public static class FilterFunction implements Function, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public Object apply(Object... params) {
            if (params != null) {
                return (Boolean) params[0];
            }
            return false;
        }
        
    }
}
