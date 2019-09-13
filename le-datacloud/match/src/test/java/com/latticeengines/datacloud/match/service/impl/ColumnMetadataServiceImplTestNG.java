package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.RefreshFrequency;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

@Component
public class ColumnMetadataServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private BeanDispatcherImpl beanDispatcher;

    @Autowired
    private DataCloudVersionService dataCloudVersionService;
    private static final String BOMBORA = "Bombora";
    private static final String HG = "HG";

    @Test(groups = "functional")
    public void testAvroSchemaForDerivedColumnsCache() {
        ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService("1.0.0");
        for (Predefined predefined : Predefined.values()) {
            Schema schema = columnMetadataService.getAvroSchema(predefined, predefined.getName(), "1.0.0");
            Assert.assertEquals(schema.getFields().size(),
                    columnMetadataService.fromPredefinedSelection(predefined, "1.0.0").size());
        }
    }

    @Test(groups = "functional")
    public void testAvroSchemaForAccountMaster() {
        String latestVersion = dataCloudVersionService.currentApprovedVersion().getVersion();
        ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService(latestVersion);
        for (Predefined predefined : Predefined.values()) {
            Schema schema = columnMetadataService.getAvroSchema(predefined, predefined.getName(), latestVersion);
            List<ColumnMetadata> columnMetadatas = columnMetadataService.fromPredefinedSelection(predefined,
                    latestVersion);
            for (ColumnMetadata columnMeta : columnMetadatas) {
                if (columnMeta.getCategory().equals(Category.TECHNOLOGY_PROFILE)) {
                    Assert.assertEquals(columnMeta.getDataLicense(), HG);
                }
                if (columnMeta.getCategory().equals(Category.INTENT)) {
                    Assert.assertEquals(columnMeta.getDataLicense(), BOMBORA);
                }
                if (columnMeta.getCategory().equals(Category.INTENT)
                        && !Boolean.TRUE.equals(columnMeta.getShouldDeprecate())) {
                    Assert.assertEquals(columnMeta.getRefreshFrequency(), RefreshFrequency.WEEK);
                } else {
                    Assert.assertEquals(columnMeta.getRefreshFrequency(), RefreshFrequency.RELEASE);
                }
            }
            Assert.assertEquals(schema.getFields().size(), columnMetadatas.size());
        }
    }

}
