package com.latticeengines.metadata.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.metadata.dao.DataCollectionDao;

@Component("dataCollectionDao")
public class DataCollectionDaoImpl extends BaseDaoImpl<DataCollection> implements DataCollectionDao {

    @Override
    protected Class<DataCollection> getEntityClass() {
        return DataCollection.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> getTableNamesOfRole(String collectionName, TableRoleInCollection tableRole) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "select tbl.name from %s as dc";
        queryPattern += " join dc.collectionTables as cTbl";
        queryPattern += " join cTbl.table as tbl";
        queryPattern += " where dc.name = :collectionName";
        if (tableRole != null) {
            queryPattern += " and cTbl.role = :tableRole";
        }
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("collectionName", collectionName);
        if (tableRole != null) {
            query.setParameter("tableRole", tableRole);
        }
        return query.list();
    }
}
