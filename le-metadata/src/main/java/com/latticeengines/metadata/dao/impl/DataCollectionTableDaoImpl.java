package com.latticeengines.metadata.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.metadata.dao.DataCollectionTableDao;

@Component("dataCollectionTableDao")
public class DataCollectionTableDaoImpl extends BaseDaoImpl<DataCollectionTable> implements DataCollectionTableDao {

    @Override
    protected Class<DataCollectionTable> getEntityClass() {
        return DataCollectionTable.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataCollectionTable findByNames(String collectionName, String tableName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "select cTbl from %s as cTbl";
        queryPattern += " join cTbl.dataCollection as dc";
        queryPattern += " join cTbl.table as tbl";
        queryPattern += " where dc.name = :collectionName";
        queryPattern += " and tbl.name = :tableName";
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("collectionName", collectionName);
        query.setParameter("tableName", tableName);
        List<DataCollectionTable> list = query.list();
        return list.stream().findFirst().orElse(null);
    }

}
