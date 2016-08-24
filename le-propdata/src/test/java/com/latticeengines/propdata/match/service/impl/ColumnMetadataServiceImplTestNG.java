package com.latticeengines.propdata.match.service.impl;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.Predefined;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

@Component
public class ColumnMetadataServiceImplTestNG extends PropDataMatchFunctionalTestNGBase {

    @Resource(name = "columnMetadataServiceDispatch")
    private ColumnMetadataService columnMetadataService;

    @Test(groups = "functional")
    public void testAvroSchema() {
        for (Predefined predefined : Predefined.values()) {
            Schema schema = columnMetadataService.getAvroSchema(predefined, predefined.getName(), null);
            Assert.assertEquals(schema.getFields().size(),
                    columnMetadataService.fromPredefinedSelection(predefined, null).size());
        }
    }

}
