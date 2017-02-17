package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.QuerySource;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableTag;
import com.latticeengines.metadata.dao.QuerySourceDao;
import com.latticeengines.metadata.entitymgr.QuerySourceEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.TableTagEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("querySourceEntityMgr")
public class QuerySourceEntityMgrImpl extends BaseEntityMgrImpl<QuerySource> implements QuerySourceEntityMgr {

    @Autowired
    private QuerySourceDao querySourceDao;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private TableTagEntityMgr tableTagEntityMgr;

    @Override
    public QuerySourceDao getDao() {
        return querySourceDao;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public QuerySource createQuerySource(List<String> tableNames, String statisticsId, boolean isDefault) {
        if (isDefault && getDefaultQuerySource() != null) {
            throw new LedpException(LedpCode.LEDP_11007);
        }

        List<Table> tables = tableNames.stream().map(tableName -> tableEntityMgr.findByName(tableName))
                .collect(Collectors.toList());
        if (tables.stream().anyMatch(table -> table == null)) {
            throw new LedpException(LedpCode.LEDP_11006, new String[] { String.join(",", tableNames) });
        }

        QuerySource querySource = new QuerySource();

        for (Table table : tables) {
            TableTag tableTag = new TableTag();
            tableTag.setTable(table);
            tableTag.setName(querySource.getName());
            tableTag.setTenantId(MultiTenantContext.getTenant().getPid());
            tableTagEntityMgr.create(tableTag);
        }

        querySource.setTenant(MultiTenantContext.getTenant());
        querySource.setDefault(isDefault);

        create(querySource);
        return getQuerySource(querySource.getName());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public QuerySource getDefaultQuerySource() {
        List<QuerySource> candidates = querySourceDao.findAllByField("isDefault", true);

        if (candidates.size() == 0) {
            return null;
        }
        QuerySource querySource = candidates.get(0);
        fillInTables(querySource);
        return querySource;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public QuerySource getQuerySource(String name) {
        List<QuerySource> candidates = querySourceDao.findAllByField("name", name);
        if (candidates.size() == 0) {
            return null;
        }
        QuerySource querySource = candidates.get(0);
        fillInTables(querySource);
        return querySource;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void removeQuerySource(String name) {
        QuerySource querySource = querySourceDao.findByField("name", name);

        // TODO Remove tags

        querySourceDao.delete(querySource);
    }

    private void fillInTables(QuerySource querySource) {
        List<Table> tables = tableTagEntityMgr.getTablesForTag(querySource.getName());
        querySource.setTables(tables);
    }

}
