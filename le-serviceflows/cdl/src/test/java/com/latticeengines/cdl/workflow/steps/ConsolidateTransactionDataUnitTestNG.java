package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ConsolidateTransactionDataUnitTestNG {

    @BeforeTest(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(groups = "unit", dataProvider = "getTableBaseDirData")
    public void getTableBaseDir(String tablePath, String expectedBaseDir) {
        ConsolidateTransactionData consolidateData = new ConsolidateTransactionData();

        Table aggregateTable = new Table();
        Extract extract = new Extract();
        extract.setPath(tablePath);
        aggregateTable.addExtract(extract);
        String baseDir = consolidateData.getTableBaseDir(aggregateTable);
        Assert.assertEquals(baseDir, expectedBaseDir);
    }

    @Test(groups = "unit")
    public void getDeltaDateFiles() {
        ConsolidateTransactionData consolidateData = new ConsolidateTransactionData();
        consolidateData.batchStoreTablePrefix = BusinessEntity.Transaction.name();
        Table aggregateTable = new Table();
        Extract extract = new Extract();
        extract.setPath("/path1/path2/path3/tableName_1992-8-2/*.avro");
        aggregateTable.addExtract(extract);
        Set<String> dates = new TreeSet<>();
        dates.add("1998-01-22");
        dates.add("1998-01-25");
        List<String> files = consolidateData.getDeltaDateFiles(aggregateTable, dates);
        Assert.assertEquals(files.size(), 2);
        Assert.assertEquals(files.get(0), "/path1/path2/path3/Transaction_1998-01-22");
        Assert.assertEquals(files.get(1), "/path1/path2/path3/Transaction_1998-01-25");
    }

    @Test(groups = "unit")
    public void createTable() {
        ConsolidateTransactionData consolidateData = new ConsolidateTransactionData();
        consolidateData.batchStoreTablePrefix = BusinessEntity.Transaction.name();
        ReflectionTestUtils.setField(consolidateData, "pipelineVersion", "2009-01-30");
        Table table = consolidateData.createTable(
                Arrays.asList("/path1/path2/path3/tableName_v1", "/path1/path2/path3/tableName_v3"), "Transaction");
        Assert.assertEquals(table.getName(), "Transaction_2009-01-30");
        Assert.assertEquals(table.getExtracts().size(), 2);
        Assert.assertEquals(table.getExtracts().get(0).getPath(), "/path1/path2/path3/tableName_v1/*.avro");
        Assert.assertEquals(table.getExtracts().get(1).getPath(), "/path1/path2/path3/tableName_v3/*.avro");
    }

    @Test(groups = "unit")
    public void getFilter() {
        ConsolidateTransactionData consolidateData = new ConsolidateTransactionData();
        HdfsFilenameFilter filter = consolidateData.getFilter();
        Assert.assertEquals(filter.accept("Transaction_2017-08-30"), true);
        Assert.assertEquals(filter.accept("Transaction_Aggregate_2017-08-30"), false);
    }

    @DataProvider(name = "getTableBaseDirData")
    public Object[][] getTableBaseDirData() {
        return new Object[][] { //
        new Object[] { "/path1/path2/path3/tableName/*.avro", "/path1/path2/path3" }, //
                new Object[] { "/path1/path2/path3/tableName/", "/path1/path2/path3" }, //
                new Object[] { "/path1/path2/path3/tableName", "/path1/path2/path3" }, //
        };
    }
}
