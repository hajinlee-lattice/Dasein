package com.latticeengines.propdata.collection.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.source.MostRecentSource;
import com.latticeengines.propdata.core.source.impl.AlexaMostRecent;

@Component
public class AlexaRefreshServiceImplTestNG extends MostRecentServiceImplTestNGBase {

    @Autowired
    AlexaRefreshService refreshService;

    @Autowired
    AlexaArchiveServiceImplTestNG archiveServiceImplDeploymentTestNG;

    @Autowired
    AlexaMostRecent source;

    @Autowired
    RefreshProgressEntityMgr progressEntityMgr;

    @Override
    RefreshService getRefreshService() {
        return refreshService;
    }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() {
        return progressEntityMgr;
    }

    @Override
    MostRecentSource getSource() {
        return source;
    }

    @Override
    CollectionArchiveServiceImplTestNGBase getBaseSourceTestBean() {
        return archiveServiceImplDeploymentTestNG;
    }

    @Override
    protected Integer getExpectedRows() {
        return 284;
    }

    @Override
    protected void verifyResultTable(RefreshProgress progress) {
        super.verifyResultTable(progress);

        String domain = "ati.com";
        Map<String, Object> row = jdbcTemplateCollectionDB
                .queryForMap("SELECT [" + source.getDomainField() + "], [US_PageViews], [US_Rank], [US_Users] FROM ["
                        + source.getSqlTableName() + "] WHERE [" + source.getDomainField() + "] = '" + domain + "'");
        Assert.assertEquals(row.get(source.getDomainField()), domain);
        Assert.assertEquals(row.get("US_PageViews"), 80.5);
        Assert.assertEquals(row.get("US_Rank"), 40493);
        Assert.assertEquals(row.get("US_Users"), 77.7);

        domain = "calendars.com";
        row = jdbcTemplateCollectionDB
                .queryForMap("SELECT [" + source.getDomainField() + "], [CA_PageViews], [CA_Rank], [CA_Users] FROM ["
                        + source.getSqlTableName() + "] WHERE [" + source.getDomainField() + "] = '" + domain + "'");
        Assert.assertEquals(row.get(source.getDomainField()), domain);
        Assert.assertEquals(row.get("CA_PageViews"), 1.5);
        Assert.assertEquals(row.get("CA_Rank"), 43206);
        Assert.assertEquals(row.get("CA_Users"), 2.0);
    }
}
