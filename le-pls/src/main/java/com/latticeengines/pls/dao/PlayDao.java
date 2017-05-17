package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Play;

public interface PlayDao extends BaseDao<Play> {

    Play findByName(String name);

    List<Play> findAllVisible();

}
