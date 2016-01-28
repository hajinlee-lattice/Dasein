package com.latticeengines.propdata.api.controller.microservice;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.propdata.ColumnMetadataProxy;

public class ColumnMetadataResourceDeploymentTest extends PropDataApiDeploymentTestNGBase{

	@Autowired
	private ColumnMetadataProxy columnMetadataProxy;

	@Test(groups = "api.deployment", enabled = true)
	public void testLeadEnrichment() {
	    /*
		List<Map<String, Object>> metadataObjs = columnMetadataProxy.columnSelection("leadenrichment");
		Assert.assertNotNull(metadataObjs);
		Assert.assertTrue(metadataObjs.size() >= 1);
		ObjectMapper mapper = new ObjectMapper();
		try {
			for (Map<String, Object> obj : metadataObjs) {
				ColumnMetadata metadata = mapper.treeToValue(mapper.valueToTree(obj), ColumnMetadata.class);
				Assert.assertTrue(metadata.getTags().contains("LeadEnrichment"));
			}
		}
		catch (IOException e) {
            Assert.fail();
        }
        */
	    List<ColumnMetadata> columnMetadataList = columnMetadataProxy.columnSelection("leadenrichment");
	    Assert.assertNotNull(columnMetadataList);
        Assert.assertTrue(columnMetadataList.size() >= 1);
	    for (ColumnMetadata columnMetadata : columnMetadataList) {
	        Assert.assertTrue(columnMetadata.getTags().contains("LeadEnrichment"));
	    }
	}

}
