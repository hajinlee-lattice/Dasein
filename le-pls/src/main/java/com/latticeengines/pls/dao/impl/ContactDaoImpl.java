package com.latticeengines.pls.dao.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Disjunction;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Contact;
import com.latticeengines.pls.dao.ContactDao;

@Component("contactDao")
public class ContactDaoImpl extends BaseDaoImpl<Contact> implements ContactDao {
 
    final int maxReturns = 256;

    @Override
    protected Class<Contact> getEntityClass() {
        return Contact.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Contact findById(String contactId) {
        Session session = getSession();
        Class<Contact> entityClz = getEntityClass();
        String queryStr = String.format("from %s where ContactId = :contactId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("contactId", contactId);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }

        return ((Contact)list.get(0));
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Contact> findContacts(Map<String, String> params) {
        Session session = getSession();
        Criteria criteria = session.createCriteria(Contact.class);
        buildRestrictions(criteria, params);
        return criteria.list();
    }

    @Override
    public Long findContactCount(Map<String, String> params) {
        Session session = getSession();
        Criteria criteria = session.createCriteria(Contact.class);
        buildRestrictions(criteria, params);
        criteria.setProjection(Projections.rowCount());
        Long count = (Long)criteria.uniqueResult();
        return count;
    }

    private void buildRestrictions(Criteria criteria, Map<String, String> params) {

        int pageSize = maxReturns;
        int pageNum = 0;
        Iterator<Map.Entry<String, String>> paramIter = params.entrySet().iterator();
        while (paramIter.hasNext()) {
            Map.Entry<String, String> param = paramIter.next();
            String key = param.getKey();
            String value = param.getValue();
            switch (key) {
               case "Page":
                   pageNum = Integer.parseInt(value.trim());
                   break;
               case "Size":
                   pageSize = Integer.parseInt(value.trim());
                   if (pageSize > maxReturns)
                       pageSize = maxReturns;
                   break;
               case "FirstName":
                   addRestriction(criteria, "firstName", value, false);
                   break;
               case "LastName":
                   addRestriction(criteria, "lastName", value, false);
                   break;
               case "CompanyID":
                   addRestriction(criteria, "companyId", value, true);
                   break;
               case "JobLevel":
                   addRestriction(criteria, "jobLevel", value, false);
                   break;
               case "JobType":
                   addRestriction(criteria, "jobType", value, false);
                   break;
               default:
                   // XXX add validation
                   break;
            }
        }
        criteria.setFirstResult(pageNum * pageSize);
        criteria.setMaxResults(pageSize);
    }

    private void addRestriction(Criteria criteria, String key, String value, boolean convertLong) {
        String[] values = value.split("#");
        if (values.length == 1) {
            if (convertLong) {
                criteria.add(Restrictions.eq(key, new Long(value)));
            } else {
                criteria.add(Restrictions.eq(key, value));
            }
        } else {
            Disjunction disj = Restrictions.disjunction();
            for (int i = 0; i < values.length; i++) {
                if (convertLong) {
                    disj.add(Restrictions.eq(key, new Long(values[i])));
                } else {
                    disj.add(Restrictions.eq(key, values[i]));
                }
            }
            criteria.add(disj);
        }
    }

    public Session getSession() throws HibernateException {
        Session sess = null;
        try {
            sess = getSessionFactory().getCurrentSession();
        } catch (HibernateException e) {
            sess = getSessionFactory().openSession();
        }
        return sess;
    }
}
