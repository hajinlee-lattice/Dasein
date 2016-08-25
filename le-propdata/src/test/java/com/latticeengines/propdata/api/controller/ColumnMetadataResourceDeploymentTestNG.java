package com.latticeengines.propdata.api.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.propdata.ColumnMetadataProxy;

@Component
public class ColumnMetadataResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Test(groups = "deployment")
    public void testPredefined() {
        for (Predefined predefined: Predefined.values()) {
            List<ColumnMetadata> columnMetadataList = columnMetadataProxy.columnSelection(predefined, null);
            Assert.assertNotNull(columnMetadataList);
            for (ColumnMetadata columnMetadata : columnMetadataList) {
                Assert.assertTrue(
                        columnMetadata.getTagList().contains(Tag.EXTERNAL),
                        "Column " + columnMetadata.getColumnName() + " does not have the tag " + Tag.EXTERNAL);
            }
        }
    }

    @Test(groups = "deployment")
    public void testLeadEnrichment() {
        List<ColumnMetadata> columnMetadataList =
                columnMetadataProxy.columnSelection(Predefined.LeadEnrichment, null);
        Assert.assertNotNull(columnMetadataList);
        for (ColumnMetadata columnMetadata : columnMetadataList) {
            Assert.assertNotNull(columnMetadata.getDisplayName());
            Assert.assertNotNull(columnMetadata.getDescription());
            Assert.assertNotNull(columnMetadata.getMatchDestination());
        }
    }
}
