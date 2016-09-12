package com.latticeengines.datacloud.match.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.CommandParameter;

public interface CommandParameterDao extends BaseDao<CommandParameter> {

    void registerParameter(String uid, String key, String value);

}
