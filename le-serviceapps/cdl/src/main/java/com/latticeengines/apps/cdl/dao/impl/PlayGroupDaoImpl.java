package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;
import com.latticeengines.apps.cdl.dao.PlayGroupDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.PlayGroup;

@Component("playGroupDao")
public class PlayGroupDaoImpl extends BaseDaoImpl<PlayGroup> implements PlayGroupDao {
    @Override
    protected Class<PlayGroup> getEntityClass() {
        return PlayGroup.class;
    }
}
