package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.PlayTypeDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.PlayType;

@Component("playTypeDao")
public class PlayTypeDaoImpl extends BaseDaoImpl<PlayType> implements PlayTypeDao {
    @Override
    protected Class<PlayType> getEntityClass() {
        return PlayType.class;
    }
}
