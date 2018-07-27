package com.latticeengines.pls.dao.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.MarketoScoringMatchField;
import com.latticeengines.pls.dao.MarketoScoringMatchFieldDao;

@Component
public class MarketoScoringMatchFieldDaoImpl extends BaseDaoImpl<MarketoScoringMatchField> implements MarketoScoringMatchFieldDao {

    @Override
    protected Class<MarketoScoringMatchField> getEntityClass() {
        return MarketoScoringMatchField.class;
    }

    @Override
    public Integer deleteFields(List<MarketoScoringMatchField> fields) {
        if (CollectionUtils.isEmpty(fields)) {
            return 0;
        }
        
        List<Long> pids = fields.stream().map(MarketoScoringMatchField::getPid).collect(Collectors.toList());
        String queryStr = String.format("DELETE FROM %s WHERE pid in (:pids)", getEntityClass().getSimpleName());
        
        Query<?> query = getCurrentSession().createQuery(queryStr).setParameter("pids", pids);
        return query.executeUpdate();
    }

}
