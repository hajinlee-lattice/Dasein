package com.latticeengines.datacloud.collection.service.impl;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.PivotService;
import com.latticeengines.datacloud.core.source.PivotedSource;
import com.latticeengines.datacloud.core.source.impl.BuiltWithPivoted;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@Component
public class BuiltWithPivotServiceImplTestNG extends PivotServiceImplTestNGBase {

    @Inject
    BuiltWithPivotService pivotService;

    @Inject
    BuiltWithPivoted source;

    @Inject
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
