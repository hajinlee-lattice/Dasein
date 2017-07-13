package com.latticeengines.matchapi.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component
public class ColumnMetadataResourceDeploymentTestNG extends MatchapiDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ColumnMetadataResourceDeploymentTestNG.class);

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Test(groups = "deployment", enabled = false)
    public void testPredefined() {
        for (Predefined predefined: Predefined.values()) {
            List<ColumnMetadata> columnMetadataList = columnMetadataProxy.columnSelection(predefined, "1.0.0");
            Assert.assertNotNull(columnMetadataList);
            for (ColumnMetadata columnMetadata : columnMetadataList) {
                Assert.assertTrue(
                        columnMetadata.getTagList().contains(Tag.EXTERNAL),
                        "Column " + columnMetadata.getColumnName() + " does not have the tag " + Tag.EXTERNAL);
            }
        }
    }

    @Test(groups = "deployment", enabled = false)
    public void testLeadEnrichment() {
        List<ColumnMetadata> columnMetadataList =
                columnMetadataProxy.columnSelection(Predefined.LeadEnrichment, "1.0.0");
        Assert.assertNotNull(columnMetadataList);
        for (ColumnMetadata columnMetadata : columnMetadataList) {
            Assert.assertNotNull(columnMetadata.getDisplayName());
            Assert.assertNotNull(columnMetadata.getDescription());
            Assert.assertNotNull(columnMetadata.getMatchDestination());
        }
    }

    @Test(groups = "deployment")
    public void testLatestVersion() {
        String dataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        Assert.assertNotNull(dataCloudVersion);
        log.info(String.format("Latest version: %s", dataCloudVersion));
    }
}
