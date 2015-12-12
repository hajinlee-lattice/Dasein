package com.latticeengines.pls.dao.impl;

import java.util.List;
import java.util.Map;
import java.util.Iterator;

import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Criteria;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Disjunction;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Company;
import com.latticeengines.pls.dao.CompanyDao;

@Component("companyDao")
public class CompanyDaoImpl extends BaseDaoImpl<Company> implements CompanyDao {
 
    final int maxReturns = 256;

    @Override
    protected Class<Company> getEntityClass() {
        return Company.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Company findById(Long companyId) {
        Session session = getSession();
        Class<Company> entityClz = getEntityClass();
        String queryStr = String.format("from %s where LatticeAccountId = :companyId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("companyId", companyId.toString());
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }

        return ((Company)list.get(0));
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Company> findCompanies(Map<String, String> params) {
        Session session = getSession();
        Criteria criteria = session.createCriteria(Company.class);
        buildRestrictions(criteria, params);
        return criteria.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Long findCompanyCount(Map<String, String> params) {
        Session session = getSession();
        Criteria criteria = session.createCriteria(Company.class);
        buildRestrictions(criteria, params);
        criteria.setProjection(Projections.rowCount());
        Long count = (Long)criteria.uniqueResult();
        return count;
    }


    private void buildRestrictions(Criteria criteria, Map<String, String> params) {

        int pageSize = maxReturns;
        int pageNum = 0;
        Iterator paramIter = params.entrySet().iterator();
        while (paramIter.hasNext()) {
            Map.Entry param = (Map.Entry) paramIter.next();
            String key = (String)param.getKey();
            String value = (String)param.getValue();
            switch (key) {
               case "Page":
                   try {
                       pageNum = Integer.parseInt(value.trim());
                   }
                   catch (NumberFormatException nfe)
                   {
                       pageNum = 0;
                   }
                   break;
               case "Size":
                   try {
                       pageSize = Integer.parseInt(value.trim());
                       if (pageSize > maxReturns) 
                           pageSize = maxReturns;
                   }
                   catch (NumberFormatException nfe)
                   {
                       pageSize = maxReturns;
                   }
                   break;
               case "Name":
                   addRestriction(criteria, "name", value);
                   break;
               case "State":
                   addRestriction(criteria, "state", value);
                   break;
               case "Country":
                   addRestriction(criteria, "country", value);
                   break;
               case "RevenueRange":
                   addRestriction(criteria, "revenueRange", value);
                   break;
               case "EmployeeRange":
                   addRestriction(criteria, "employeeRange", value);
                   break;
               case "Industry":
                   addRestriction(criteria, "industry", value);
                   break;
               case "SubIndustry":
                   addRestriction(criteria, "subIndustry", value);
                   break;
               default:
                   // XXX add validation 
                   break;
            }
        }
        criteria.setFirstResult(pageNum * pageSize);
        criteria.setMaxResults(pageSize);
    }
   
    private void addRestriction(Criteria criteria, String key, String value) { 
        String[] values = value.split("#");
        if (values.length == 1) { 
            criteria.add(Restrictions.eq(key, value));
        } else {
            Disjunction disj = Restrictions.disjunction();
            for (int i = 0; i < values.length; i++) {
                disj.add(Restrictions.eq(key, values[i]));
            }
            criteria.add(disj);
        }
    }

    private Session getSession() throws HibernateException {
        Session sess = null;
        try {
            sess = getSessionFactory().getCurrentSession();
        } catch (HibernateException e) {
            sess = getSessionFactory().openSession();
        }
        return sess;
    }
}
