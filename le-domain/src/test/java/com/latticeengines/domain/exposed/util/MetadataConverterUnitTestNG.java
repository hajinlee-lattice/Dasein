package com.latticeengines.domain.exposed.util;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Table;

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
    public void testBucketedTable() {
        String path = getResourceAbsolutePath(
                "com/latticeengines/domain/exposed/util/metadataConverterUnitTestNG/am.avsc");
        Table table = MetadataConverter.getBucketedTableFromSchemaPath(configuration, path, null, null);
        assertEquals(table.getAttributes().size(), 134);
        assertEquals(table.getAttribute("Attr1_8").getBucketList().toString(),
                Arrays.asList(new String[] { null, "Value1", "Value2", "Value3"}).toString());
    }
}
