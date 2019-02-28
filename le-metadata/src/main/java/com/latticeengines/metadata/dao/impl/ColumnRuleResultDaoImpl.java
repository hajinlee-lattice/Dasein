package com.latticeengines.metadata.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.metadata.dao.ColumnRuleResultDao;

@Component("columnRuleResultDao")
public class ColumnRuleResultDaoImpl extends BaseDaoImpl<ColumnRuleResult> implements ColumnRuleResultDao {

    @Override
    protected Class<ColumnRuleResult> getEntityClass() {
        return ColumnRuleResult.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ColumnRuleResult> findByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ColumnRuleResult> entityClz = getEntityClass();
        String queryStr = String.format("from %s where modelId = :modelId", entityClz.getSimpleName());
        Query<ColumnRuleResult> query = session.createQuery(queryStr);
        query.setParameter("modelId", modelId);
        List<ColumnRuleResult> list = query.list();
        if (list.size() == 0) {
            return new ArrayList<>();
        }
        return list;
    }

}
