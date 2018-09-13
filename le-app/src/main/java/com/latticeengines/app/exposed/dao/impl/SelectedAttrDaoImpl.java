package com.latticeengines.app.exposed.dao.impl;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.dao.SelectedAttrDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.SelectedAttribute;

@Component("enrichmentAttrDao")
public class SelectedAttrDaoImpl extends BaseDaoImpl<SelectedAttribute> implements SelectedAttrDao {

    @Override
    protected Class<SelectedAttribute> getEntityClass() {
        return SelectedAttribute.class;
    }

    @Override
    public Integer count(boolean onlyPremium) {
        Session session = getSessionFactory().getCurrentSession();
        Class<SelectedAttribute> entityClz = getEntityClass();
        String basicQueryStr = "select count(*) from %s ";
        if (onlyPremium) {
            basicQueryStr += " where isPremium = :isPremium ";
        }
        String queryStr = String.format(basicQueryStr, entityClz.getSimpleName());
        @SuppressWarnings("rawtypes")
        Query query = session.createQuery(queryStr);
        if (onlyPremium) {
            query.setParameter("isPremium", onlyPremium);
        }
        return ((Long) query.uniqueResult()).intValue();
    }
}
