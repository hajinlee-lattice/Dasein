package com.latticeengines.propdata.api.controller.microservice;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;

public class ColumnMetadataResourceFunctionalTestNG extends PropDataApiFunctionalTestNGBase {
	private static final String PROPDATA_METADATA_PREDEFINED = "propdata/metadata/predefined";

	@SuppressWarnings("unchecked")
    @Test(groups = "api.functional")
	public void testLeadEnrichment() {
		String url = getRestAPIHostPort() + PROPDATA_METADATA_PREDEFINED + "/leadenrichment";
		List<Map<String, Object>> metadataObjs = restTemplate.getForObject(url, List.class);
		Assert.assertNotNull(metadataObjs);
		Assert.assertTrue(metadataObjs.size() >= 1);
		ObjectMapper mapper = new ObjectMapper();
		try {
			for (Map<String, Object> obj : metadataObjs) {
				ColumnMetadata metadata = mapper.treeToValue(mapper.valueToTree(obj), ColumnMetadata.class);
				Assert.assertTrue(metadata.getTagList().contains("LeadEnrichment"));
			}
		}
		catch (IOException e) {
            Assert.fail();
        }
	}
}
