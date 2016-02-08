package com.latticeengines.propdata.collection.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.core.source.PivotedSource;
import com.latticeengines.propdata.core.source.impl.BuiltWithPivoted;

@Component
public class BuiltWithPivotServiceImplTestNG extends PivotServiceImplTestNGBase {

    @Autowired
    BuiltWithPivotService pivotService;

    @Autowired
    BuiltWithPivoted source;

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
        return 48;
    }

    @Override
    protected void verifyResultTable(RefreshProgress progress) {
        super.verifyResultTable(progress);

        String domain = "bloomfw.com";
        Map<String, Object> row = jdbcTemplateCollectionDB.queryForMap("SELECT [" + source.getDomainField()
                + "], [BusinessTechnologiesDocinfo], [BusinessTechnologiesFramework], [BusinessTechnologiesWidgets], "
                + "[TechIndicator_Apache] FROM [" + source.getSqlTableName() + "] WHERE [" + source.getDomainField()
                + "] = '" + domain + "'");
        Assert.assertEquals(row.get(source.getDomainField()), domain);
        Assert.assertEquals(row.get("BusinessTechnologiesDocinfo"), 10);
        Assert.assertEquals(row.get("BusinessTechnologiesFramework"), 5);
        Assert.assertEquals(row.get("BusinessTechnologiesWidgets"), 6);
        Assert.assertEquals(row.get("TechIndicator_Apache"), "No");

        domain = "sandhuniforms.com";
        row = jdbcTemplateCollectionDB.queryForMap("SELECT [" + source.getDomainField()
                + "], [BusinessTechnologiesAnalytics], [BusinessTechnologiesFramework], [TechIndicator_Apache] FROM ["
                + source.getSqlTableName() + "] WHERE [" + source.getDomainField() + "] = '" + domain + "'");

        Assert.assertEquals(row.get(source.getDomainField()), domain);
        Assert.assertEquals(row.get("BusinessTechnologiesAnalytics"), 2);
        Assert.assertEquals(row.get("BusinessTechnologiesFramework"), 2);
        Assert.assertEquals(row.get("TechIndicator_Apache"), "Yes");
    }

}
