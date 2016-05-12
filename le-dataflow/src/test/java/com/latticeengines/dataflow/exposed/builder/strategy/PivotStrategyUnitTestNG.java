package com.latticeengines.dataflow.exposed.builder.strategy;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public class PivotStrategyUnitTestNG {

    @Test(groups = "unit", dataProvider = "pivotToClassData")
    public void testPivotToClass(Class<? extends Serializable> targetClass) {
        Set<String> keys = new HashSet<>(Arrays.asList("a", "b"));
        PivotStrategyImpl pivot = PivotStrategyImpl.any("key", "value", keys, targetClass, null);
        List<FieldMetadata> fieldMetadataList = pivot.getFieldMetadataList();
        for (FieldMetadata metadata : fieldMetadataList) {
            Assert.assertTrue(keys.contains(metadata.getFieldName()));
            Assert.assertEquals(metadata.getJavaType(), targetClass);
        }
    }

    @DataProvider(name = "pivotToClassData")
    private Object[][] pivotToClassData() {
        return new Object[][] { { Integer.class }, { String.class }, { Float.class }, { Double.class }, { Long.class },
                { Boolean.class } };
    }

    @Test(groups = "unit", dataProvider = "notNullData", expectedExceptions = IllegalArgumentException.class)
    public void testNotNull(String key, String value, Set<String> keys) {
        PivotStrategyImpl.any(key, value, keys, Integer.class, 0);
    }

    @DataProvider(name = "notNullData")
    private Object[][] notNullData() {
        return new Object[][] { { "key", "value", null }, { "key", "value", new HashSet<>() },
                { "key", "", new HashSet<>(Arrays.asList("a", "b")) },
                { "key", null, new HashSet<>(Arrays.asList("a", "b")) },
                { "", "value", new HashSet<>(Arrays.asList("a", "b")) },
                { null, "value", new HashSet<>(Arrays.asList("a", "b")) }, };
    }

}
