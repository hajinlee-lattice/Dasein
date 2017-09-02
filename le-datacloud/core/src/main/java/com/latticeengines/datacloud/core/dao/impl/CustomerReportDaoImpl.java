package com.latticeengines.datacloud.core.dao.impl;


import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.dao.CustomerReportDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;

@Component("customerReportDao")
public class CustomerReportDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<CustomerReport> implements CustomerReportDao {

    @Override
    protected Class<CustomerReport> getEntityClass() {
        return CustomerReport.class;
    }

    @Override
    public CustomerReport findById(String id) {
        Session session = getSessionFactory().getCurrentSession();
        Class<CustomerReport> entityClz = getEntityClass();
        String queryStr = String
                .format("from %s where ID = :id",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("id", id);
        return (CustomerReport) query.list().get(0);
    }

}
