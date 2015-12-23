package com.latticeengines.dataflow.exposed.builder.strategy;

import static com.latticeengines.dataflow.exposed.builder.DataFlowBuilder.FieldMetadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotResult;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;

public class PivotStrategyUnitTestNG {

    @Test(groups = "unit", dataProvider = "pivotToClassData")
    public void testPivotToClass(Class<?> targetClass) {
        Set<String> keys = new HashSet<>(Arrays.asList("a", "b"));
        PivotStrategyImpl pivot = PivotStrategyImpl.pivotToClass("key", "value", keys, targetClass);
        List<FieldMetadata> fieldMetadataList = pivot.getFieldMetadataList();
        for (FieldMetadata metadata: fieldMetadataList) {
            Assert.assertTrue(keys.contains(metadata.getFieldName()));
            Assert.assertEquals(metadata.getJavaType(), targetClass);
        }
    }

    @DataProvider(name = "pivotToClassData")
    private Object[][] pivotToClassData() {
        return new Object[][] {
                {Integer.class},
                {String.class},
                {Float.class},
                {Double.class},
                {Long.class},
                {Boolean.class}
        };
    }

    @Test(groups = "unit", dataProvider = "notNullData", expectedExceptions = IllegalArgumentException.class)
    public void testNotNull(String key, String value, Set<String> keys) {
        PivotStrategyImpl.pivotToClass(key, value, keys, Integer.class);
    }

    @DataProvider(name = "notNullData")
    private Object[][] notNullData() {
        return new Object[][] {
                {"key", "value", null},
                {"key", "value", new HashSet<>()},
                {"key", "", new HashSet<>(Arrays.asList("a", "b"))},
                {"key", null, new HashSet<>(Arrays.asList("a", "b"))},
                {"", "value", new HashSet<>(Arrays.asList("a", "b"))},
                {null, "value", new HashSet<>(Arrays.asList("a", "b"))},
        };
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testColumnMappingCollide() {
        Set<String> keys = new HashSet<>(Arrays.asList("a", "b", "c"));
        Map<String, String> cMap = new HashMap<>();
        cMap.put("a", "a");
        cMap.put("b", "b");
        cMap.put("c", "a");

        Map<String, Integer> pMap = new HashMap<>();
        pMap.put("c", 1);
        PivotStrategyImpl pivot = new PivotStrategyImpl("key", "value", keys, Integer.class, cMap, pMap, null, null, null);
        Assert.assertNotNull(pivot);
        PivotResult result = pivot.pivot("c");
        Assert.assertEquals(result.getPriority(), 1);

        new PivotStrategyImpl("key", "value", keys, Integer.class,
                cMap, null, null, null, null);
    }

    @Test(groups = "unit")
    public void testPivot() {
        Set<String> keys = new HashSet<>(Arrays.asList("a", "b", "c"));
        PivotStrategyImpl pivot = PivotStrategyImpl.pivotToClass("key", "value", keys, Integer.class);
        PivotResult result = pivot.pivot("a");
        Assert.assertEquals(result.getColumnName(), "a");
        Assert.assertEquals(result.getPriority(), PivotStrategyImpl.DEFAULT_PRIORITY);
        Assert.assertNull(pivot.pivot("nope"));
    }

}
