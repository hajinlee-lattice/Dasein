package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.pls.dao.PlayDao;

@Component("playDao")
public class PlayDaoImpl extends BaseDaoImpl<Play> implements PlayDao {

    @Override
    protected Class<Play> getEntityClass() {
        return Play.class;
    }

    @Override
    public Play findByName(String name) {
        return super.findByField("NAME", name);
    }

}
