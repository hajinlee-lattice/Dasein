package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.query.factory.PrestoQueryProvider.PRESTO_USER;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;

public class PrestoEntityQueryServiceImplTestNG extends EntityQueryServiceImplTestNG {

    @Inject
    private PrestoConnectionService prestoConnectionService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestDataWithSpark(3);
    }

    @Override
    @DataProvider(name = "userContexts", parallel = false)
    protected Object[][] provideSqlUserContexts() {
        if (prestoConnectionService.isPrestoDbAvailable()) {
            return new Object[][]{ //
                    {PRESTO_USER, "Presto"}
            };
        } else {
            return new Object[][]{};
        }
    }

    @DataProvider(name = "timefilterProvider")
    protected Object[][] timefilterProvider() {
        return getTimeFilterDataProvider(PRESTO_USER);
    }

}
