package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataCollectionTableDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;

@Component("dataCollectionTableDao")
public class DataCollectionTableDaoImpl extends BaseDaoImpl<DataCollectionTable> implements DataCollectionTableDao {

    @Override
    protected Class<DataCollectionTable> getEntityClass() {
        return DataCollectionTable.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<DataCollectionTable> findAllByName(String collectionName, String tableName, DataCollection.Version version) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "select cTbl from %s as cTbl";
        queryPattern += " join cTbl.dataCollection as dc";
        queryPattern += " join cTbl.table as tbl";
        queryPattern += " where dc.name = :collectionName";
        queryPattern += " and tbl.name = :tableName";
        if (version != null) {
            queryPattern += " and cTbl.version = :version";
        }
        queryPattern += " order by cTbl.pid desc";
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query<DataCollectionTable> query = session.createQuery(queryStr);
        query.setParameter("collectionName", collectionName);
        query.setParameter("tableName", tableName);
        if (version != null) {
            query.setParameter("version", version);
        }
        return query.list();
    }

}
