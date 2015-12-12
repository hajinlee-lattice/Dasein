package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Criteria;
import org.hibernate.criterion.Restrictions;
import org.hibernate.Session;
import org.springframework.stereotype.Component;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.AmAttribute;
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
        log.info("Search result " + attrs.toString());
        return attrs;
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
