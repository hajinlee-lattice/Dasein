package com.latticeengines.propdata.api.controller.microservice;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.propdata.ColumnMetadataProxy;

public class ColumnMetadataResourceDeploymentTest extends PropDataApiDeploymentTestNGBase {

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Test(groups = "api.deployment", enabled = true)
    public void testLeadEnrichment() {
        List<ColumnMetadata> columnMetadataList = columnMetadataProxy
                .columnSelection(ColumnSelection.Predefined.LEAD_ENRICHMENT);
        Assert.assertNotNull(columnMetadataList);
        System.out.println("Total number of available LeadEnrichment attributes: " + columnMetadataList.size());
        for (ColumnMetadata columnMetadata : columnMetadataList) {
            Assert.assertTrue(
                    columnMetadata.getTagList().contains(ColumnSelection.Predefined.LEAD_ENRICHMENT.getName()),
                    "Column " + columnMetadata.getColumnName() + " does not have the tage LeadEnrichment");
        }
    }
}
