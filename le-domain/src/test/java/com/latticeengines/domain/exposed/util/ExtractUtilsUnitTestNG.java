package com.latticeengines.domain.exposed.util;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;

public class ExtractUtilsUnitTestNG {

    public static final String RESOURCE_BASE = "com/latticeengines/domain/exposed/util/extractUtilsUnitTest";

    @Test(groups = "unit")
    public void testGlob() throws Exception {
        Table table = new Table();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        File file = resolver.getResource(RESOURCE_BASE).getFile();
        Extract e1 = new Extract();
        e1.setPath(file.getPath() + "/*.avro");
        table.addExtract(e1);
        List<String> paths = ExtractUtils.getExtractPaths(new Configuration(), table);
        assertEquals(paths.size(), 3);
    }

    @Test(groups = "unit")
    public void testDirectory() throws Exception {
        Table table = new Table();
        Extract e1 = new Extract();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        File file = resolver.getResource(RESOURCE_BASE).getFile();
        e1.setPath(file.getPath());
        table.addExtract(e1);
        List<String> paths = ExtractUtils.getExtractPaths(new Configuration(), table);
        assertEquals(paths.size(), 3);
    }
}
