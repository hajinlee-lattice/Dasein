package com.latticeengines.metadata.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.Table;

public interface TableDao extends BaseDao<Table> {

    Table findByName(String name);

    List<Table> findAllWithExpireRetentionPolicy(int index, int max);

}
