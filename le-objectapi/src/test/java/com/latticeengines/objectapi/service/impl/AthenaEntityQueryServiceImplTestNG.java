package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.query.factory.AthenaQueryProvider.ATHENA_USER;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

public class AthenaEntityQueryServiceImplTestNG extends EntityQueryServiceImplTestNG {

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestData(3);
    }

    @Override
    @DataProvider(name = "userContexts", parallel = false)
    protected Object[][] provideSqlUserContexts() {
        return new Object[][]{ //
                {ATHENA_USER, "Athena"}
        };
    }

    @DataProvider(name = "timefilterProvider")
    protected Object[][] timefilterProvider() {
        return getTimeFilterDataProvider(ATHENA_USER);
    }

}
