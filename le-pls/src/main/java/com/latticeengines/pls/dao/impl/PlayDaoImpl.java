package com.latticeengines.pls.dao.impl;

import java.util.List;

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

    @Override
    public List<Play> findAllByRatingEnginePid(long pid) {
        return super.findAllByField("FK_RATING_ENGINE_ID", pid);
    }

}
