package com.latticeengines.propdata.core.entitymgr.impl;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.core.entitymgr.ExternalColumnEntityMgr;
import com.latticeengines.propdata.core.testframework.PropDataCoreFunctionalTestNGBase;


@Component
public class ExternalColumnEntityMgrImplTestNG extends PropDataCoreFunctionalTestNGBase {

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
    
    @Test(groups = "functional", enabled = true)
    public void testGetLeadEnrichment() throws IOException {
    	Date start = new Date();
    	List<ExternalColumn>columns = externalColumnEntityMgr.getLeadEnrichment();
    	Date end = new Date();
    	System.out.println("Total rows: " + columns.size());
    	System.out.println("Duration: " + TimeUnit.MILLISECONDS.toMillis(end.getTime() - start.getTime()));
    	ObjectMapper mapper = new ObjectMapper();
        for (ExternalColumn column: columns.subList(0, 1)) {
            System.out.println(mapper.writeValueAsString(column));
        }
    	
    }

}
