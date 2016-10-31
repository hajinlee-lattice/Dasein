package com.latticeengines.datacloud.match.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dao.CountryCodeDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.CountryCode;

@Component("countryCodeDao")
public class CountryCodeDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<CountryCode> implements CountryCodeDao {
    @Override
    protected Class<CountryCode> getEntityClass() {
        return CountryCode.class;
    }
}
