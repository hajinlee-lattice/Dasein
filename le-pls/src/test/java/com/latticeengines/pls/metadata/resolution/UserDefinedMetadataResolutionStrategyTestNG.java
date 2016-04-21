package com.latticeengines.pls.metadata.resolution;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

public class UserDefinedMetadataResolutionStrategyTestNG extends PlsFunctionalTestNGBaseDeprecated {

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

        Set<String> expectedUnknownColumns = Sets.newHashSet(new String[] { "Some Column" });
        List<ColumnTypeMapping> mappings = strategy.getUnknownColumns();
        assertEquals(mappings.size(), expectedUnknownColumns.size());
        for (ColumnTypeMapping mapping : mappings) {
            assertTrue(expectedUnknownColumns.contains(mapping.getColumnName()));
            assertEquals(mapping.getColumnType(), Type.STRING.name());
        }

        strategy = new UserDefinedMetadataResolutionStrategy(hdfsPath, SchemaInterpretation.SalesforceAccount,
                mappings, yarnConfiguration);
        strategy.calculate();
        Table table = strategy.getMetadata();

        assertEquals(table.getAttribute(InterfaceName.Id).getDisplayName(), "Account ID");
        assertEquals(table.getAttribute(InterfaceName.Website).getDisplayName(), "Website");
        assertEquals(table.getAttribute(InterfaceName.Event).getDisplayName(), "Event");
        assertEquals(table.getAttribute(InterfaceName.Country).getDisplayName(), "Billing Country");
        assertEquals(table.getAttribute(InterfaceName.CompanyName).getDisplayName(), "Account Name");
        assertEquals(table.getAttribute(InterfaceName.LastModifiedDate).getDisplayName(), "Last Modified Date");
        assertNull(table.getAttribute(InterfaceName.AnnualRevenue));
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
         HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
    }
}
