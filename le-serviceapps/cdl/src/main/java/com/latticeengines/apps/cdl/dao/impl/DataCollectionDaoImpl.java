package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataCollectionDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

@Component("dataCollectionDao")
public class DataCollectionDaoImpl extends BaseDaoImpl<DataCollection> implements DataCollectionDao {

    @Override
    protected Class<DataCollection> getEntityClass() {
        return DataCollection.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> getTableNamesOfRole(String collectionName, TableRoleInCollection tableRole,
            DataCollection.Version version) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "select tbl.name from %s as dc";
        queryPattern += " join dc.collectionTables as cTbl";
        queryPattern += " join cTbl.table as tbl";
        queryPattern += " where dc.name = :collectionName";
        if (tableRole != null) {
            queryPattern += " and cTbl.role = :tableRole";
        }
        if (version != null) {
            queryPattern += " and cTbl.version = :version";
        }
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("collectionName", collectionName);
        if (tableRole != null) {
            query.setParameter("tableRole", tableRole);
        }
        if (version != null) {
            query.setParameter("version", version);
        }
        return query.list();
    }


    @SuppressWarnings("unchecked")
    @Override
    public List<Object[]> findTableNamesOfAllRole(String collectionName){
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "select tbl.name,cTbl.role,cTbl.version from %s as dc";
        queryPattern += " join dc.collectionTables as cTbl";
        queryPattern += " join cTbl.table as tbl";
        queryPattern += " where dc.name = :collectionName";
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("collectionName", collectionName);
        return query.list();
    }
}
