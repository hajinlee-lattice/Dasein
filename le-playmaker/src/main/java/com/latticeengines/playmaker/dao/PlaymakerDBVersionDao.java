package com.latticeengines.playmaker.dao;

import com.latticeengines.db.exposed.dao.GenericDao;

public interface PlaymakerDBVersionDao extends GenericDao {

    String getDBVersion();

}
