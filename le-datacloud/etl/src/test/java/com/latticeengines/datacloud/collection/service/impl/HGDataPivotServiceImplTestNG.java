package com.latticeengines.datacloud.collection.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.PivotService;
import com.latticeengines.datacloud.core.source.PivotedSource;
import com.latticeengines.datacloud.core.source.impl.HGDataPivoted;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

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
