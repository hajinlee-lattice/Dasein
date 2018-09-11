package com.latticeengines.dante.testframework.testdao;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.PlayType;

@Component("testPlayTypeDao")
public class TestPlayTypeDao extends BaseDaoImpl<PlayType> {
    @Override
    protected Class<PlayType> getEntityClass() {
        return PlayType.class;
    }
}
