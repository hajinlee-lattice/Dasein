package com.latticeengines.metadata.dao.impl;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Root;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
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
        String queryPattern = "select count(*) from %s as attr where attr.table.pid = :tablePid";
        String queryStr = String.format(queryPattern, entityClz.getSimpleName());
        Query<Long> query = session.createQuery(queryStr, Long.class);
        query.setParameter("tablePid", tablePid);
        return query.getSingleResult();
    }

    @Override
    public List<Attribute> findByTablePid(Long tablePid, Pageable pageable) {
        Session session = getSessionFactory().getCurrentSession();
        CriteriaBuilder builder = session.getCriteriaBuilder();
        CriteriaQuery<Attribute> criteriaQuery = buildPageableCriteria(tablePid, builder, pageable);
        Query<Attribute> query = session.createQuery(criteriaQuery);
        if (pageable != null) {
            query.setMaxResults(pageable.getPageSize());
            query.setFirstResult(pageable.getPageNumber() * pageable.getPageSize());
        }
        return query.list();
    }

    private CriteriaQuery<Attribute> buildPageableCriteria(Long tablePid, CriteriaBuilder builder, Pageable pageable) {
        CriteriaQuery<Attribute> criteria = builder.createQuery(Attribute.class);
        Root<Attribute> root = criteria.from(Attribute.class);
        if (pageable != null) {
            List<Order> orders = new ArrayList<>();
            org.springframework.data.domain.Sort pageSort = pageable.getSort();
            if (pageSort.isSorted()) {
                for (Sort.Order pageOrder : pageSort) {
                    Order order = pageOrder.isAscending() //
                            ? builder.asc(root.get(pageOrder.getProperty())) //
                            : builder.desc(root.get(pageOrder.getProperty()));
                    orders.add(order);
                }
            }
            if (CollectionUtils.isNotEmpty(orders)) {
                criteria.orderBy(orders);
            }
        }
        return criteria.select(root).where(builder.equal(root.get("table").get("pid"), tablePid));
    }

}
