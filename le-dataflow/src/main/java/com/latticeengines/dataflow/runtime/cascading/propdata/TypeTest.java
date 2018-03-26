package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.common.exposed.util.JsonUtils;

import java.util.ArrayList;
import java.util.List;

public class TypeTest {
    public static void main(String[] argv) {
        List<Class<?>> classList = new ArrayList<>();
        classList.add(String.class);
        classList.add(Boolean.class);
        classList.add(Integer.class);
        classList.add(Double.class);

        Class<Double> another = Double.class;

        System.out.println(classList.get(3) == another);

        System.out.println(JsonUtils.serialize(another));
    }
}
