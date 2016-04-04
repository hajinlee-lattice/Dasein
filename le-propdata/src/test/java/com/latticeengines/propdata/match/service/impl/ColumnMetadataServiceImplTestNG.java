package com.latticeengines.propdata.match.service.impl;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

@Component
public class ColumnMetadataServiceImplTestNG  extends PropDataMatchFunctionalTestNGBase {

    @Autowired
    private ColumnMetadataService columnMetadataService;

    @Test(groups = "functional")
    public void testAvroSchema() {
        for (ColumnSelection.Predefined predefined: ColumnSelection.Predefined.values()) {
            Schema schema = columnMetadataService.getAvroSchema(predefined, predefined.getName());
            Assert.assertEquals(schema.getFields().size(),
                    columnMetadataService.fromPredefinedSelection(predefined).size());
        }
    }

}
