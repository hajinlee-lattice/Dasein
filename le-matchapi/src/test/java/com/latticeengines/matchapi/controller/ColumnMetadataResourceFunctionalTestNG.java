package com.latticeengines.matchapi.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.matchapi.testframework.MatchapiFunctionalTestNGBase;

@Component
public class ColumnMetadataResourceFunctionalTestNG extends MatchapiFunctionalTestNGBase {

    private static final String METADATA_PREDEFINED = "/match/metadata/predefined";

    @SuppressWarnings("unchecked")
    @Test(groups = { "functional" })
    public void testPredefined() {
        for (Predefined predefined: Predefined.values()) {
            String url = getRestAPIHostPort() + METADATA_PREDEFINED + "/" + predefined.name();
            List<Map<String, Object>> metadataObjs = restTemplate.getForObject(url, List.class);
            Assert.assertNotNull(metadataObjs);
            ObjectMapper mapper = new ObjectMapper();
            try {
                for (Map<String, Object> obj : metadataObjs) {
                    ColumnMetadata metadata = mapper.treeToValue(mapper.valueToTree(obj), ColumnMetadata.class);
                    Assert.assertTrue(metadata.getTagList().contains(Tag.EXTERNAL),
                            "Column " + metadata.getColumnName() + " does not have the tag " + Tag.EXTERNAL);
                }
            } catch (IOException e) {
                Assert.fail();
            }
        }

    }
}
