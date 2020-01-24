package com.latticeengines.domain.exposed.util;

public final class CategoryNameUtils {

    protected CategoryNameUtils() {
        throw new UnsupportedOperationException();
    }

    public static String getCategoryName(String category, String subcategory) {
        return String.format("%s.%s", category, subcategory).toString();
    }
}
