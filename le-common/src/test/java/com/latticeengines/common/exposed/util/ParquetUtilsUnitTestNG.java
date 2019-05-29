package com.latticeengines.common.exposed.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ParquetUtilsUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(ParquetUtilsUnitTestNG.class);

    private String tempDir = "/tmp/ParquetUnitTest";

    @BeforeClass(groups = "unit")
    public void setup() {
        copyToTmpDir();
    }

    @Test(groups = "unit")
    public void testReadParquet() {
        long count = ParquetUtils.countParquetFiles(new Configuration(), tempDir + "/*.parquet");
        Assert.assertEquals(count, 3L);
        try (ParquetUtils.ParquetFilesIterator iterator = //
                ParquetUtils.iteratorParquetFiles(new Configuration(), tempDir + "/*.parquet")) {
            iterator.forEachRemaining(record -> log.info(record.toString()));
        }
    }

    private void copyToTmpDir() {
        FileUtils.deleteQuietly(new File(tempDir));
        Resource[] resources = getTestResources();
        for (Resource resource: resources) {
            String fileName = resource.getFilename();
            log.info("Copy " + fileName + " to tmp dir");
            try {
                FileUtils.copyInputStreamToFile(resource.getInputStream(), new File(tempDir + "/" + fileName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Resource[] getTestResources() {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        String pattern = "com/latticeengines/common/exposed/util/parquetUtilsData/Output1/*";
        try {
            return resolver.getResources(pattern);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
