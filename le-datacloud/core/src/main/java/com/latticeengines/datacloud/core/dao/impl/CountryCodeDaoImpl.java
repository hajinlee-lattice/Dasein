package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.dao.CountryCodeDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.CountryCode;

@Component("countryCodeDao")
public class CountryCodeDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<CountryCode> implements CountryCodeDao {
    @Override
    protected Class<CountryCode> getEntityClass() {
        return CountryCode.class;
    }

    @Override
    public String findCountryCode(String country) {
        Session session = getSessionFactory().getCurrentSession();
        Class<CountryCode> entityClz = getEntityClass();
        String queryStr = String.format("from %s where CountryName = :country", entityClz.getSimpleName());
        Query<CountryCode> query = session.createQuery(queryStr, CountryCode.class);
        query.setParameter("country", country);
        List<CountryCode> list = query.list();
        if (CollectionUtils.isEmpty(list)) {
            return null;
        } else {
            return list.get(0).getIsoCountryCode2Char();
        }
    }

    @Override
    public String findCountry(String country) {
        Session session = getSessionFactory().getCurrentSession();
        Class<CountryCode> entityClz = getEntityClass();
        String queryStr = String.format("from %s where CountryName = :country", entityClz.getSimpleName());
        Query<CountryCode> query = session.createQuery(queryStr, CountryCode.class);
        query.setParameter("country", country);
        List<CountryCode> list = query.list();
        if (CollectionUtils.isEmpty(list)) {
            return null;
        } else {
            return list.get(0).getIsoCountryName();
        }
    }
}
