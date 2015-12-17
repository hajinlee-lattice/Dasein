package com.latticeengines.propdata.collection.testframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class PropDataCollectionFunctionalTestNGBase extends PropDataCollectionAbstractTestNGBase {

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplate")
    protected JdbcTemplate jdbcTemplate;
}
