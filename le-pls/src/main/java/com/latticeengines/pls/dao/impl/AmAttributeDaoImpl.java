package com.latticeengines.pls.dao.impl;

import java.util.List;
import java.lang.reflect.Field;


import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Criteria;
import org.hibernate.criterion.Restrictions;
import org.hibernate.Session;
import org.springframework.stereotype.Component;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.AmAttribute;
import com.latticeengines.domain.exposed.pls.Company;

import com.latticeengines.pls.dao.AmAttributeDao;

@Component("amAttributeDao")
public class AmAttributeDaoImpl extends BaseDaoImpl<AmAttribute> implements AmAttributeDao {

    private static final Log log = LogFactory.getLog(AmAttributeDaoImpl.class);
    private final int maxReturns = 256;


    @Override
    protected Class<AmAttribute> getEntityClass() {
        return AmAttribute.class;
    }

    @SuppressWarnings("unchecked")
    public List<AmAttribute> findAttributes(String key, String parentKey, String parentValue) {
        Session session = getSession();
        Criteria criteria = session.createCriteria(AmAttribute.class);

        criteria.add(Restrictions.eq("attrKey",  key));
        if (parentKey != null) {
            criteria.add(Restrictions.eq("parentKey",  parentKey));
        }
        if (parentValue != null) {
            criteria.add(Restrictions.eq("parentValue",  parentValue));
        }
        List<AmAttribute> attrs = criteria.list();
        return attrs;
    }

    @SuppressWarnings("unchecked")
    public AmAttribute findAttributeMeta(String key) {
        Session session = getSession();
        Criteria criteria = session.createCriteria(AmAttribute.class);

        criteria.add(Restrictions.eq("attrValue",  key));
        criteria.add(Restrictions.eq("attrKey",  "_KEY_"));
        List<AmAttribute> attrs = criteria.list();
        log.info("Search result " + attrs.toString());
        return attrs.get(0);
    }

    @SuppressWarnings("unchecked")
    public List<List> findCompanyCount(String key, String parentKey, String parentValue) {
        Query query;
        Session session = getSession();
        Class<AmAttribute> attrEntityClz = getEntityClass();

        if (parentKey != null) {
            String keyField = convertPropToField(Company.class, key);
            String queryStr = String.format("select new list(attr.attrValue, count(*)) from %s attr, %s company " +
                                            "where attr.attrKey = :attrKey and %s = attr.attrValue and attr.parentKey = :parentKey " +
                                            "and attr.parentValue =:parentValue " +
                                            " group by attr.attrValue ",
                                             attrEntityClz.getSimpleName(),
                                             Company.class.getSimpleName(),
                                             keyField);
             query = session.createQuery(queryStr);
             query.setString("attrKey", key);
             query.setString("parentKey", parentKey);
             query.setString("parentValue", parentValue);

        } else {
            String keyField = convertPropToField(Company.class, key);
            String queryStr = String.format("select new list(attr.attrValue, count(*)) from %s attr, %s company " +
                                            "where attr.attrKey = :attrKey and company.%s = attr.attrValue " +
                                            "group by attr.attrValue ",
                                             attrEntityClz.getSimpleName(),
                                             Company.class.getSimpleName(),
                                             keyField);
            log.info("query string: " + queryStr);
            query = session.createQuery(queryStr);
            query.setString("attrKey", key);
        }
        List list = query.list();
        return list;
    }

    private String convertPropToField(Class entityClass, String property) {
         String fieldName = property;
         for (Field f : entityClass.getDeclaredFields()) {
             if (f.getName().equalsIgnoreCase(property)) {
                 fieldName = f.getName();
                 break;
             }
         }
         return fieldName;
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
