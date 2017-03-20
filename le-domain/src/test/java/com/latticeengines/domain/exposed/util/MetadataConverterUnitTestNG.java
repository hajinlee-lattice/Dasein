package com.latticeengines.domain.exposed.util;

import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.JdbcStorage.DatabaseName;

public class MetadataConverterUnitTestNG {
    private Configuration configuration = new Configuration();

    private String getResourceAbsolutePath(String resourcePath) {
        try {
            ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource resource = resolver.getResource(resourcePath);
            return resource.getFile().getAbsolutePath();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = "unit")
    public void testMultipleExtractsWhenProvideDirectory() throws Exception {
        String path = getResourceAbsolutePath(
                "com/latticeengines/domain/exposed/util/metadataConverterUnitTestNG/extracts");
        Table table = MetadataConverter.getTable(configuration, path);
        String extractDir = table.getExtractsDirectory();
        assertEquals(extractDir, path);
    }

    @Test(groups = "unit")
    public void testMultipleExtractsWhenProvideGlob() throws Exception {
        String path = getResourceAbsolutePath(
                "com/latticeengines/domain/exposed/util/metadataConverterUnitTestNG/extracts");
        Table table = MetadataConverter.getTable(configuration, path + "/*.avro");
        String extractDir = table.getExtractsDirectory();
        assertEquals(extractDir, path);
    }

    @Test(groups = "unit")
    public void testBucketedTable() throws IOException {
        String path = getResourceAbsolutePath(
                "com/latticeengines/domain/exposed/util/metadataConverterUnitTestNG/am.avsc");
        Table bucketedTable = MetadataConverter.getBucketedTableFromSchemaPath(configuration, path, null, null);
        JdbcStorage storage = new JdbcStorage();
        storage.setDatabaseName(DatabaseName.REDSHIFT);
        storage.setTableNameInStorage("redshift_bucketedaccountmaster");
        bucketedTable.setStorageMechanism(storage);

        // FileUtils.write(new File("bucketedaccountmastertable.json"),
        // bucketedTable.toString());
        assertEquals(bucketedTable.getAttributes().size(), 134);
        assertEquals(bucketedTable.getAttribute("Attr1_8").getBucketRangeList().size(), 4);
        assertEquals(bucketedTable.getAttribute("Attr1_8").getBucketRangeList().get(0).isNullOnly(),
                Boolean.TRUE.booleanValue());
        assertEquals(bucketedTable.getAttribute("Attr1_8").getBucketRangeList().get(1).getMin(), "Value1");
        assertEquals(bucketedTable.getAttribute("Attr1_8").getBucketRangeList().get(2).getMax(), "Value2");
    }
}
