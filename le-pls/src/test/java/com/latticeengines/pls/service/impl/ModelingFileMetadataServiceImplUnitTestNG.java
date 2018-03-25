package com.latticeengines.pls.service.impl;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.domain.exposed.pls.frontend.RequiredType;

public class ModelingFileMetadataServiceImplUnitTestNG {

    private ModelingFileMetadataServiceImpl modelingFileMetadataService = new ModelingFileMetadataServiceImpl();

    @Test(groups = "unit")
    public void checkGetLatticeSchemaFields() {
        List<LatticeSchemaField> latticeSchemaFields = modelingFileMetadataService
                .getSchemaToLatticeSchemaFields(SchemaInterpretation.Account);
        Assert.assertNotNull(latticeSchemaFields);
        Assert.assertTrue(latticeSchemaFields.size() > 0);
        LatticeSchemaField idField = null;
        for (LatticeSchemaField latticeSchemaField : latticeSchemaFields) {
            if (latticeSchemaField.getName().equals(InterfaceName.AccountId.name())) {
                idField = latticeSchemaField;
                break;
            }
        }
        Assert.assertNotNull(idField);
        Assert.assertEquals(idField.getRequiredType(), RequiredType.Required);
    }
}
