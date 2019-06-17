package com.latticeengines.matchapi.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.matchapi.testframework.MatchapiFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

// dpltc deploy -a matchapi
public class ColumnMetadataResourceDeploymentTestNG extends MatchapiFunctionalTestNGBase {

    private static final String METADATA_PREDEFINED = "/match/metadata/predefined";

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountMasterColumnService;

    @Inject
    private DataCloudVersionService datacloudVersionService;

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" })
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
                            "Column " + metadata.getAttrName() + " does not have the tag " + Tag.EXTERNAL);
                }
            } catch (IOException e) {
                Assert.fail();
            }
            
            List<ColumnMetadata> cms = columnMetadataProxy.columnSelection(predefined);
            cms.forEach(cm -> {
                Assert.assertTrue(cm.getTagList().contains(Tag.EXTERNAL),
                        "Column " + cm.getAttrName() + " does not have the tag " + Tag.EXTERNAL);
            });
        }
    }

    @Test(groups = { "deployment" })
    public void testGetAllColumns() {
        // Test default datacloud version
        List<ColumnMetadata> cms = columnMetadataProxy.getAllColumns();
        Assert.assertTrue(CollectionUtils.isNotEmpty(cms));
        String datacloudVersion = datacloudVersionService.currentApprovedVersion().getVersion();
        List<AccountMasterColumn> amColumns = accountMasterColumnService.scan(datacloudVersion, null, null) //
                .sequential().collectList().block();
        Assert.assertEquals(cms.size(), amColumns.size());
    }
}
