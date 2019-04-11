package com.latticeengines.apps.cdl.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
            queryPattern += " and cTbl.role = :getTableRole";
        }
        if (version != null) {
            queryPattern += " and cTbl.version = :version";
        }
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        @SuppressWarnings("rawtypes")
        Query query = session.createQuery(queryStr);
        query.setParameter("collectionName", collectionName);
        if (tableRole != null) {
            query.setParameter("getTableRole", tableRole);
        }
        if (version != null) {
            query.setParameter("version", version);
        }
        return query.list();
    }


    @SuppressWarnings("rawtypes")
    @Override
    public Map<TableRoleInCollection, Map<DataCollection.Version, List<String>>> findTableNamesOfAllRole(String collectionName, TableRoleInCollection tableRole, DataCollection.Version version){
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "select tbl.name,cTbl.role,cTbl.version from %s as dc";
        queryPattern += " join dc.collectionTables as cTbl";
        queryPattern += " join cTbl.table as tbl";
        queryPattern += " where dc.name = :collectionName";
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("collectionName", collectionName);

        Map<TableRoleInCollection, Map<DataCollection.Version, List<String>>> tableRoleNames = new HashMap<>();
        for(Iterator iterator = query.list().iterator(); iterator.hasNext();){
            Object[] objects = (Object[]) iterator.next();
            String name = objects[0].toString();
            TableRoleInCollection role = (TableRoleInCollection)objects[1];
            DataCollection.Version ver = (DataCollection.Version)objects[2];
            if (!tableRoleNames.containsKey(role)){
                tableRoleNames.put(role,new HashMap<>());
            }
            Map<DataCollection.Version,List<String>> versionTableNamesMap = tableRoleNames.get(role);
            if (!versionTableNamesMap.containsKey(ver)){
                versionTableNamesMap.put(ver,new ArrayList<>());
            }
            versionTableNamesMap.get(ver).add(name);
        }
        return tableRoleNames;
    }
}
