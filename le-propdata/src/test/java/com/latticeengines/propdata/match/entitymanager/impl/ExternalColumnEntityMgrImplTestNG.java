package com.latticeengines.propdata.match.entitymanager.impl;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.entitymanager.ExternalColumnEntityMgr;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;


@Component
public class ExternalColumnEntityMgrImplTestNG extends PropDataMatchFunctionalTestNGBase {

    @Autowired
    private ExternalColumnEntityMgr externalColumnEntityMgr;
    
    @Test(groups = "functional", enabled = true)
    public void testGetColumnsInPredefined() throws IOException {
        for (ColumnSelection.Predefined selection: ColumnSelection.Predefined.values()) {
            Date start = new Date();
            String tag = selection.getName();
            List<ExternalColumn> columns = externalColumnEntityMgr.findByTag(tag);
            Date end = new Date();
            System.out.println("Total rows: " + columns.size());
            System.out.println("Duration: " + TimeUnit.MILLISECONDS.toMillis(end.getTime() - start.getTime()) + " msec.");

            Assert.assertFalse(columns.isEmpty(), "Should find more than 0 columns");
            ObjectMapper mapper = new ObjectMapper();
            for (ExternalColumn column: columns.subList(0, 1)) {
                System.out.println(mapper.writeValueAsString(column));
                Assert.assertTrue(column.getTagList().contains(tag));
            }
        }
    }

}
