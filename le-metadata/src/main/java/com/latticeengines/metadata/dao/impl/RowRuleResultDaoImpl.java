package com.latticeengines.metadata.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.metadata.dao.RowRuleResultDao;

@Component("rowRuleResultDao")
public class RowRuleResultDaoImpl extends BaseDaoImpl<RowRuleResult> implements RowRuleResultDao {

    @Override
    protected Class<RowRuleResult> getEntityClass() {
        return RowRuleResult.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RowRuleResult> findByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<RowRuleResult> entityClz = getEntityClass();
        String queryStr = String.format("from %s where modelId = :modelId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        List<RowRuleResult> list = query.list();
        if (list.size() == 0) {
            return new ArrayList<>();
        }
        return list;
    }

}
