package com.latticeengines.skald;

import java.lang.reflect.Method;

public enum ScoreType {

    PROBABILITY(Double.class),

    LIFT(Double.class),

    PERCENTILE(Integer.class),

    BUCKET(String.class);

    private ScoreType(Class<?> type) {
        this.type = type;
    }

    private final Class<?> type;

    public Class<?> type() {
        return type;
    }
    
    public static Object parse(ScoreType scoretype, String rawvalue) {
        if (rawvalue == null) {
            return null;
        }
        
        Class<?> clazz = scoretype.type();
        String underlying = clazz.getSimpleName();
        Method method;
        try {
            switch (underlying) {
            case "Double":
                method = clazz.getMethod("parseDouble", String.class);
                return method.invoke(null, rawvalue);
            case "Integer":
                method = clazz.getMethod("parseInteger", String.class);
                return method.invoke(null, rawvalue);
            case "String":
                return rawvalue;
            default:
                throw new UnsupportedOperationException("Unsupported underlying type " + underlying);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failure parsing value " + rawvalue + " to ScoreType " + scoretype);
        }
    }
    
    public static boolean valueEquals(ScoreType scoretype, Object value1, Object value2) {
        Class<?> clazz = scoretype.type();
        if (clazz == Double.class) {
            return Math.abs(((Double)value1).doubleValue() - ((Double)value2).doubleValue()) < 1e-10; 
        }
        return value1.equals(value2);
    }
}