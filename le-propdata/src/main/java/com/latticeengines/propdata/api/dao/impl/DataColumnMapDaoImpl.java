package com.latticeengines.propdata.api.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.propdata.api.dao.DataColumnMapDao;

@Component("dataColumnMapDao")
public class DataColumnMapDaoImpl extends BaseDaoImpl<DataColumnMap> implements DataColumnMapDao {

    @Override
    protected Class<DataColumnMap> getEntityClass() {
        return DataColumnMap.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public DataColumnMap findByContent(String extensionName, String sourceTableName) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataColumnMap> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Extension_Name = :extensionName AND SourceTableName = :sourceTableName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("extensionName", extensionName);
        query.setString("sourceTableName",sourceTableName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (DataColumnMap)list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<DataColumnMap> findByColumnCalcIds(List<Long> ids) {
        Session session = sessionFactory.getCurrentSession();
        Class<DataColumnMap> entityClz = getEntityClass();
        if (ids.isEmpty()) { return new ArrayList<>(); }

        String idsStr = "(";
        for (Long id: ids) {
            idsStr += String.valueOf(id) + ",";
        }
        idsStr = idsStr.substring(0, idsStr.length() - 1) + ")";
        String queryStr = String.format("from %s where ColumnCalcID IN %s", entityClz.getSimpleName(), idsStr);
        Query query = session.createQuery(queryStr);
        return query.list();
    }

}
