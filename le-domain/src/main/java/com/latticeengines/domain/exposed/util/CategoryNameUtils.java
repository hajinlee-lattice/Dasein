package com.latticeengines.domain.exposed.util;

public class CategoryNameUtils {

    public static String getCategoryName(String category, String subcategory) {
        return String.format("%s.%s", category, subcategory).toString();
    }
}
