package com.latticeengines.datacloud.match.dao.impl;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.hibernate.Query;
import org.hibernate.Session;
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

    @SuppressWarnings("unchecked")
    @Override
    public String findByCountry(String country) {
        Session session = getSessionFactory().getCurrentSession();
        Class<CountryCode> entityClz = getEntityClass();
        String queryStr = String.format("from %s where CountryName = :country", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("country", country);
        List<CountryCode> list = query.list();
        if (CollectionUtils.isEmpty(list)) {
            return null;
        } else {
            return list.get(0).getIsoCountryCode2Char();
        }
    }
}
