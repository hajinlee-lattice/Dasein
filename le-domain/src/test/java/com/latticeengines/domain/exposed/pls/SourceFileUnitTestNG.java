package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class SourceFileUnitTestNG {

    @Test(groups = "unit", enabled = false)
    public void testSerDe() {
        SourceFile sourceFile = new SourceFile();
        sourceFile.setTenant(new Tenant("tenant"));
        sourceFile.setSchemaInterpretation(SchemaInterpretation.SalesforceAccount);
        sourceFile.setState(SourceFileState.Uploaded);
        sourceFile.setBusinessEntity(BusinessEntity.Account);
        sourceFile.setCreated(new Date());
        sourceFile.setUpdated(new Date());

        String serialized = JsonUtils.serialize(sourceFile);
        System.out.println(serialized);
    }

}
