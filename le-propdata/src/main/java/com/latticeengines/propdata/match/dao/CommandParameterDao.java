package com.latticeengines.propdata.match.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.CommandParameter;

public interface CommandParameterDao extends BaseDao<CommandParameter> {

    void registerParameter(String uid, String key, String value);

}
