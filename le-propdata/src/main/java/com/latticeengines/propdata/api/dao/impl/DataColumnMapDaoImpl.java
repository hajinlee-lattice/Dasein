package com.latticeengines.propdata.api.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.propdata.api.dao.DataColumnMapDao;

public class DataColumnMapDaoImpl extends BaseDaoImpl<DataColumnMap> implements
        DataColumnMapDao {

    public DataColumnMapDaoImpl() {
        super();
    }

    @Override
    protected Class<DataColumnMap> getEntityClass() {
        return DataColumnMap.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public DataColumnMap findByContent(String extensionName,
            String sourceTableName) {
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

}
