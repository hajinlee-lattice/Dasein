package com.latticeengines.pls.metadata.resolution;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class UserDefinedMetadataResolutionStrategyTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    String hdfsPath = "/tmp/test_metadata_resolution";

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        String path = ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv").getPath();

        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, path, hdfsPath);
    }

    @Test(groups = "functional")
    public void getUnknowColumns() {
        UserDefinedMetadataResolutionStrategy strategy = new UserDefinedMetadataResolutionStrategy(hdfsPath,
                SchemaInterpretation.SalesforceAccount, null, yarnConfiguration);
        strategy.calculate();

        Set<String> expectedUnknownColumns = Sets.newHashSet(new String[] { "Column3", "Column4", "Column5",
                "LastUpdatedDate" });
        List<ColumnTypeMapping> mappings = strategy.getUnknownColumns();
        assertEquals(mappings.size(), expectedUnknownColumns.size());
        for (ColumnTypeMapping mapping : mappings) {
            assertTrue(expectedUnknownColumns.contains(mapping.getColumnName()));
            assertEquals(mapping.getColumnType(), Type.STRING.name());
        }
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
    }
}
