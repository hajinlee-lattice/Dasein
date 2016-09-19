package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.pls.dao.MarketoMatchFieldDao;

@Component("marketoMatchFieldDao")
public class MarketoMatchFieldDaoImpl extends BaseDaoImpl<MarketoMatchField>
        implements MarketoMatchFieldDao {

    @Override
    protected Class<MarketoMatchField> getEntityClass() {
        return MarketoMatchField.class;
    }

}
