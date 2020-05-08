package com.latticeengines.apps.cdl.util;

public final class AttributeSetContext {

    protected AttributeSetContext() {
        throw new UnsupportedOperationException();
    }

    private static ThreadLocal<String> attributeSetNameThreadLocal = new ThreadLocal<>();

    public static void setAttributeSetName(String attributeSetName) {
        attributeSetNameThreadLocal.set(attributeSetName);
    }

    public static String getAttributeSetName() {
        return attributeSetNameThreadLocal.get();
    }

}
