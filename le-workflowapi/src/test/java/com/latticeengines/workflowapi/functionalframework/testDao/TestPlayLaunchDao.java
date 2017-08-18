package com.latticeengines.workflowapi.functionalframework.testDao;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@Component("testPlayLaunchDao")
public class TestPlayLaunchDao extends BaseDaoImpl<PlayLaunch> {
    @Override
    protected Class<PlayLaunch> getEntityClass() {
        return PlayLaunch.class;
    }
}
