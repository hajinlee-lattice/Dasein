package com.latticeengines.common.exposed.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StackTraceUtils {

    protected StackTraceUtils() {
        throw new UnsupportedOperationException();
    }
    
    public static final String LE_PATTERN = "com.latticeengines";
    private static final String NEW_LINE_TAB = System.getProperty("line.separator") + "\t";
    
    public static List<String> getCurrentStackTrace() {
        return getCurrentStackTrace(true);
    }

    public static List<String> getCurrentStackTrace(boolean applyLatticeFilter) {
        Stream<StackTraceElement> stStream = Arrays.asList(Thread.currentThread().getStackTrace()).parallelStream();
        if (applyLatticeFilter) {
            stStream = stStream.filter(st -> st != null && st.toString().startsWith(LE_PATTERN));
        }   
        return stStream.map(st -> NEW_LINE_TAB + st.toString()).collect(Collectors.toList());
    }
    
}
