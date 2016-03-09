package com.latticeengines.propdata.api.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.propdata.ColumnMetadataProxy;

@Component
public class ColumnMetadataResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Test(groups = "deployment", enabled = true)
    public void testPredefined() {
        for (ColumnSelection.Predefined predefined: ColumnSelection.Predefined.values()) {
            List<ColumnMetadata> columnMetadataList = columnMetadataProxy.columnSelection(predefined);
            Assert.assertNotNull(columnMetadataList);
            for (ColumnMetadata columnMetadata : columnMetadataList) {
                Assert.assertTrue(
                        columnMetadata.getTagList().contains(predefined.getName()),
                        "Column " + columnMetadata.getColumnName() + " does not have the tag " + predefined.getName());
            }
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testLeadEnrichment() {
        List<ColumnMetadata> columnMetadataList =
                columnMetadataProxy.columnSelection(ColumnSelection.Predefined.LeadEnrichment);
        Assert.assertNotNull(columnMetadataList);
        for (ColumnMetadata columnMetadata : columnMetadataList) {
            Assert.assertNotNull(columnMetadata.getDisplayName());
            Assert.assertNotNull(columnMetadata.getDescription());
            Assert.assertNotNull(columnMetadata.getMatchDestination());
        }
    }
}
