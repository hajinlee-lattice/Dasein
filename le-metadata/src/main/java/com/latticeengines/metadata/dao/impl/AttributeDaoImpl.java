package com.latticeengines.metadata.dao.impl;

import java.util.Iterator;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Attribute;

import com.latticeengines.metadata.dao.AttributeDao;

@Component("attributeDao")
public class AttributeDaoImpl extends BaseDaoImpl<Attribute> implements AttributeDao {

    @Override
    protected Class<Attribute> getEntityClass() {
        return Attribute.class;
    }

    @Override
    public Long countByTablePid(Long tablePid) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Attribute> entityClz = getEntityClass();
        Criteria criteria = session.createCriteria(getEntityClass());
        criteria.add(Restrictions.eq("table.pid", tablePid));
        criteria.setProjection(Projections.rowCount());
        
        return (Long) criteria.uniqueResult();
    }
    
    @Override
    public List<Attribute> findByTablePid(Long tablePid, Pageable pageable) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Attribute> entityClz = getEntityClass();
        Criteria criteria = session.createCriteria(getEntityClass());
        criteria.add(Restrictions.eq("table.pid", tablePid));
        buildPageableCriteria(criteria, pageable);
        
        @SuppressWarnings("unchecked")
        List<Attribute> attributes = criteria.list();
        return attributes;
    }

    protected void buildPageableCriteria(Criteria criteria, Pageable pageable) {
        if (criteria == null || pageable == null) {
            return;
        }
        
        criteria.setMaxResults(pageable.getPageSize());
        criteria.setFirstResult(pageable.getPageNumber() * pageable.getPageSize());
        
        org.springframework.data.domain.Sort pageSort = pageable.getSort();
        if (pageSort != null && pageSort.isSorted()) {
            Iterator<org.springframework.data.domain.Sort.Order> sortOrderIter = pageSort.iterator();
            while(sortOrderIter.hasNext()) {
                org.springframework.data.domain.Sort.Order pageOrder = sortOrderIter.next();
                criteria.addOrder(pageOrder.isAscending() ? Order.asc(pageOrder.getProperty()) : Order.desc(pageOrder.getProperty()));
            }
        }
    }
    
}
