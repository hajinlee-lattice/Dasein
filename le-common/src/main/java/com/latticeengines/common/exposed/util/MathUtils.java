package com.latticeengines.common.exposed.util;

import java.util.Arrays;
import java.util.List;

public class MathUtils {
    public static <T extends Comparable<T>> T min(List<T> list) {
        T min = null;
        for (T val : list) {
            if (min == null || val.compareTo(min) < 0) {
                min = val;
            }
        }
        return min;
    }

    public static <T extends Comparable<T>> T max(List<T> list) {
        T max = null;
        for (T val : list) {
            if (max == null || val.compareTo(max) > 0) {
                max = val;
            }
        }
        return max;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<T>> T min(T... vals) {
        return min(Arrays.asList(vals));
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<T>> T max(T... vals) {
        return max(Arrays.asList(vals));
    }
}
