package com.latticeengines.propdata.collection.testframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class PropDataCollectionDeploymentTestNGBase extends PropDataCollectionAbstractTestNGBase {

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplateSrc")
    protected JdbcTemplate jdbcTemplateSrc;
}
