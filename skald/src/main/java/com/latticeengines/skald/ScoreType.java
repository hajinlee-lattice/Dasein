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
}