package com.latticeengines.domain.exposed.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

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
    public void testExtractsWithDifferentSchemaNotSupported() {
        String path = getResourceAbsolutePath("com/latticeengines/domain/exposed/util/metadataConverterUnitTestNG/mixedExtracts");
        boolean thrown = false;
        try {
            MetadataConverter.getTable(configuration, path);
        } catch (Exception e) {
            thrown = true;
        }
        assertTrue(thrown);
    }
}
