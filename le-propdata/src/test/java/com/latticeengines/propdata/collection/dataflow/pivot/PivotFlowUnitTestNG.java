package com.latticeengines.propdata.collection.dataflow.pivot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.domain.exposed.propdata.collection.SourceColumn;
import com.latticeengines.domain.exposed.propdata.collection.SourceColumn.Calculation;

public class PivotFlowUnitTestNG {

    private PivotFlow pivotFlow = new PivotFlow();

    @Test(groups = "unit")
    public void testSimplePivotGroups() {
        Object[][] data = new Object[][] {
                {"Column1", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key1\"}"},
                {"Column2", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key2\"}"},
                {"Column3", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key3\"}"},
        };
        List<SourceColumn> columns = parseData(data);
        Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap = pivotFlow.getPivotStrategyMap(columns);
        Assert.assertEquals(pivotStrategyMap.size(), 1);
        PivotStrategyImpl impl = (PivotStrategyImpl) getFirstStrategyFromMap(pivotStrategyMap);
        Assert.assertNotNull(impl);
        Assert.assertEquals(impl.keyColumn, "Key");
    }

    @Test(groups = "unit")
    public void testDifferentColumnTypeSamePivot() {
        Object[][] data = new Object[][] {
                {"Column1", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key1\"}"},
                {"Column2", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key2\"}"},
                {"Column3", "BIGINT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key3\"}"},
        };
        List<SourceColumn> columns = parseData(data);
        Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap = pivotFlow.getPivotStrategyMap(columns);
        Assert.assertEquals(pivotStrategyMap.size(), 1);
    }

    @Test(groups = "unit")
    public void testDifferentPivotTypeSamePivot() {
        Object[][] data = new Object[][] {
                {"Column1", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key1\"}"},
                {"Column2", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key2\"}"},
                {"Column3", "BIGINT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MIN,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key3\"}"},
        };
        List<SourceColumn> columns = parseData(data);
        Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap = pivotFlow.getPivotStrategyMap(columns);
        Assert.assertEquals(pivotStrategyMap.size(), 1);
    }

    @Test(groups = "unit")
    public void testDifferentGroupByDifferentPivot() {
        Object[][] data = new Object[][] {
                {"Column1", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key1\"}"},
                {"Column2", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key2\",\"DefaultValue\":0}"},
                {"Column3", "INT", "Base1", "GroupBy1", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key3\"}"},
        };
        List<SourceColumn> columns = parseData(data);
        Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap = pivotFlow.getPivotStrategyMap(columns);
        Assert.assertEquals(pivotStrategyMap.size(), 2);
    }

    @Test(groups = "unit")
    public void testDifferentBaseDifferentPivot() {
        Object[][] data = new Object[][] {
                {"Column1", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key1\"}"},
                {"Column2", "INT", "Base1", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key2\"}"},
                {"Column3", "INT", "Base2", "GroupBy1,GroupBy2", Calculation.PIVOT_MAX,
                        "{\"PivotKeyColumn\":\"Key\",\"PivotValueColumn\":\"Value\",\"TargetPivotKeys\":\"Key3\"}"},
        };
        List<SourceColumn> columns = parseData(data);
        Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap = pivotFlow.getPivotStrategyMap(columns);
        Assert.assertEquals(pivotStrategyMap.size(), 2);
    }

    private List<SourceColumn> parseData(Object[][] data) {
        List<SourceColumn> columns = new ArrayList<>();
        for (Object[] record: data) {
            SourceColumn column = new SourceColumn();
            column.setColumnName((String) record[0]);
            column.setColumnType((String) record[1]);
            column.setBaseSource((String) record[2]);
            column.setGroupBy((String) record[3]);
            column.setCalculation((Calculation) record[4]);
            column.setArguments((String) record[5]);
            columns.add(column);
        }
        return columns;
    }

    private PivotStrategy getFirstStrategyFromMap(Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap) {
        for (Map.Entry<ImmutableList<String>, PivotStrategy> enty: pivotStrategyMap.entrySet()) {
            return enty.getValue();
        }
        return null;
    }

    @SuppressWarnings("unused")
    private ImmutableList<String> getFirstKeyFromMap(Map<ImmutableList<String>, PivotStrategy> pivotStrategyMap) {
        for (Map.Entry<ImmutableList<String>, PivotStrategy> enty: pivotStrategyMap.entrySet()) {
            return enty.getKey();
        }
        return null;
    }
}
