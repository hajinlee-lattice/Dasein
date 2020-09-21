package com.latticeengines.apps.dcp.dao.impl;

import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.DataReportDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

@Component("dataReportDao")
public class DataReportDaoImpl extends BaseDaoImpl<DataReportRecord> implements DataReportDao {
    @Override
    protected Class<DataReportRecord> getEntityClass() {
        return DataReportRecord.class;
    }

    @Override
    public void deleteDataReportRecords(Set<Long> pids) {
        if (CollectionUtils.isEmpty(pids)) {
            return;
        }
        String queryStr = String.format("DELETE FROM %s WHERE pid in (:pids)", getEntityClass().getSimpleName());

        Query<?> query = getCurrentSession().createQuery(queryStr).setParameter("pids", pids);
        query.executeUpdate();
    }
}
