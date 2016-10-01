package com.latticeengines.datacloud.match.service.impl;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

@Component
public class ColumnMetadataServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private BeanDispatcherImpl beanDispatcher;

    @Test(groups = "functional")
    public void testAvroSchemaForDerivedColumnsCache() {
        ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService("1.0.0");
        for (Predefined predefined : Predefined.values()) {
            Schema schema = columnMetadataService.getAvroSchema(predefined, predefined.getName(), null);
            Assert.assertEquals(schema.getFields().size(),
                    columnMetadataService.fromPredefinedSelection(predefined, null).size());
        }
    }

    @Test(groups = "functional")
    public void testAvroSchemaForAccountMaster() {
        ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService("2.0.0");
        for (Predefined predefined : Predefined.values()) {
            Schema schema = columnMetadataService.getAvroSchema(predefined, predefined.getName(), null);
            Assert.assertEquals(schema.getFields().size(),
                    columnMetadataService.fromPredefinedSelection(predefined, null).size());
        }
    }

}
