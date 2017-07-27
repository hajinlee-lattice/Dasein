package com.latticeengines.dante.testFramework.testDao;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Play;

@Component("testPlayDao")
public class TestPlayDao extends BaseDaoImpl<Play> {
    @Override
    protected Class<Play> getEntityClass() {
        return Play.class;
    }
}
