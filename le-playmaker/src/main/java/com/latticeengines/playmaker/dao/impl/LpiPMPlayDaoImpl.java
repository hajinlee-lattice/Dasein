package com.latticeengines.playmaker.dao.impl;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.playmaker.dao.LpiPMPlayDao;

@Component("lpiPMPlayDao")
public class LpiPMPlayDaoImpl //
        extends BaseDaoImpl<Play> //
        implements LpiPMPlayDao {

    @Override
    protected Class<Play> getEntityClass() {
        return Play.class;
    }

    @Override
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds) {
        return null;
    }

    @Override
    public int getPlayCount(long start, List<Integer> playgroupIds) {
        return 0;
    }

}
