package com.latticeengines.domain.exposed.util;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.JdbcStorage.DatabaseName;
import com.latticeengines.domain.exposed.metadata.Table;

public class MetadataConverterUnitTestNG {
    private Configuration configuration;

    @BeforeClass(groups = "unit")
    void init()
    {
        configuration = new Configuration();
        configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    }
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
        String path = getResourceAbsolutePath("com/latticeengines/domain/exposed/util/metadataConverterUnitTestNG/extracts");
        Table table = MetadataConverter.getTable(configuration, path);
        String extractDir = table.getExtractsDirectory();
        assertEquals(extractDir, path);
    }

    @Test(groups = "unit")
    public void testMultipleExtractsWhenProvideGlob() throws Exception {
        String path = getResourceAbsolutePath("com/latticeengines/domain/exposed/util/metadataConverterUnitTestNG/extracts");
        Table table = MetadataConverter.getTable(configuration, path + "/*.avro");
        String extractDir = table.getExtractsDirectory();
        assertEquals(extractDir, path);
    }

    @Test(groups = "unit")
    public void testBucketedTable() throws IOException {
//        String path = getResourceAbsolutePath("com/latticeengines/domain/exposed/util/metadataConverterUnitTestNG/am.avsc");
//        Table bucketedTable = MetadataConverter.getBucketedTableFromSchemaPath(configuration, path, null, null);
//        JdbcStorage storage = new JdbcStorage();
//        storage.setDatabaseName(DatabaseName.REDSHIFT);
//        storage.setTableNameInStorage("redshift_bucketedaccountmaster");
//        bucketedTable.setStorageMechanism(storage);

//        assertEquals(bucketedTable.getAttributes().size(), 16483);
//        Attribute attribute = bucketedTable.getAttribute("TechIndicator_AdRoll");
//        assertNotNull(attribute);
//        List<BucketRange> bucketRangeList = attribute.getBucketRangeList();
//        assertNotNull(bucketRangeList);
//        assertEquals(bucketRangeList.size(), 3);
//        assertTrue(bucketRangeList.get(0).isNullOnly());
//        assertEquals(bucketRangeList.get(1).getMin(), "Yes");
//        assertEquals(bucketRangeList.get(1).getMax(), "Yes");
//        assertEquals(bucketRangeList.get(2).getMin(), "No");
//        assertEquals(bucketRangeList.get(2).getMax(), "No");
    }
}
