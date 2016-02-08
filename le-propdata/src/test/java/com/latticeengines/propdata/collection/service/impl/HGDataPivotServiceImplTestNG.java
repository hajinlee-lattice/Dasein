package com.latticeengines.propdata.collection.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.core.source.PivotedSource;
import com.latticeengines.propdata.core.source.impl.HGDataPivoted;

@Component
public class HGDataPivotServiceImplTestNG extends PivotServiceImplTestNGBase {

    @Autowired
    HGDataPivotService pivotService;

    @Autowired
    HGDataPivoted source;

    @Autowired
    RefreshProgressEntityMgr progressEntityMgr;

    @Override
    PivotService getPivotService() {
        return pivotService;
    }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() {
        return progressEntityMgr;
    }

    @Override
    PivotedSource getSource() {
        return source;
    }

    @Override
    Integer getExpectedRows() {
        return 142;
    }

    @Override
    protected void verifyResultTable(RefreshProgress progress) {
        super.verifyResultTable(progress);

        String domain = "rutgers.edu";
        Map<String, Object> row = jdbcTemplateCollectionDB.queryForMap("SELECT [" + source.getDomainField()
                + "], [CloudTechnologies_ApplicationDevelopmentMgmt], [CloudTechnologies_ATS], [CloudTechnologies_DataCenterSolutions_One], "
                + "[TechIndicator_AutodeskAutoCAD] FROM [" + source.getSqlTableName() + "] WHERE ["
                + source.getDomainField() + "] = '" + domain + "'");
        Assert.assertEquals(row.get(source.getDomainField()), domain);
        Assert.assertEquals(row.get("CloudTechnologies_ApplicationDevelopmentMgmt"), 2);
        Assert.assertEquals(row.get("CloudTechnologies_ATS"), 1);
        Assert.assertEquals(row.get("CloudTechnologies_DataCenterSolutions_One"), 5);
        Assert.assertEquals(row.get("TechIndicator_AutodeskAutoCAD"), "No");

        domain = "stewartsshops.com";
        row = jdbcTemplateCollectionDB.queryForMap("SELECT [" + source.getDomainField()
                + "], [CloudTechnologies_DatabaseManagementSoftware], [CloudTechnologies_DataCenterSolutions_One], [TechIndicator_AutodeskAutoCAD] FROM ["
                + source.getSqlTableName() + "] WHERE [" + source.getDomainField() + "] = '" + domain + "'");

        Assert.assertEquals(row.get(source.getDomainField()), domain);
        Assert.assertEquals(row.get("CloudTechnologies_DatabaseManagementSoftware"), 2);
        Assert.assertEquals(row.get("CloudTechnologies_DataCenterSolutions_One"), 5);
        Assert.assertEquals(row.get("TechIndicator_AutodeskAutoCAD"), "Yes");
    }

}
