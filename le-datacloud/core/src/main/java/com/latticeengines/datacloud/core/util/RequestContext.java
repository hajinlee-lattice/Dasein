package com.latticeengines.datacloud.core.util;

import java.util.ArrayList;
import java.util.List;

public class RequestContext {

    private static ThreadLocal<List<String>> errors = new ThreadLocal<>();

    public static void logError(String error) {
        if (errors.get() == null) {
            errors.set(new ArrayList<>());
        }
        errors.get().add(error);
    }

    public static List<String> getErrors() {
        return errors.get();
    }

    public static void cleanErrors() {
        errors.remove();
    }
}
