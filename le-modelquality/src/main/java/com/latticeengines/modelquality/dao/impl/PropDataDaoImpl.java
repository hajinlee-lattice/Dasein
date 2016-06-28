package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.dao.PropDataDao;

@Component("propDataDao")
public class PropDataDaoImpl extends BaseDaoImpl<PropData> implements PropDataDao {

    @Override
    protected Class<PropData> getEntityClass() {
        return PropData.class;
    }

}
