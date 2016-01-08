package com.latticeengines.propdata.collection.entitymanager.impl;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.propdata.ExternalColumn;
import com.latticeengines.propdata.collection.entitymanager.ExternalColumnEntityMgr;
import com.latticeengines.propdata.collection.testframework.PropDataCollectionFunctionalTestNGBase;


@Component
public class ExternalColumnEntityMgrImplTestNG extends PropDataCollectionFunctionalTestNGBase {

    @Autowired
    private ExternalColumnEntityMgr externalColumnEntityMgr;

    @Test(groups = "functional", enabled = false)
    public void testFindAll() throws IOException {
        List<ExternalColumn> columns = externalColumnEntityMgr.getExternalColumns();
        ObjectMapper mapper = new ObjectMapper();
        for (ExternalColumn column: columns.subList(0, 1)) {
            System.out.println(mapper.writeValueAsString(column));
        }
    }

}
