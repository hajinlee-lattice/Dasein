package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableTag;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.metadata.dao.TableTagDao;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.TableTagEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("tableTagEntityMgr")
public class TableTagEntityMgrImpl extends BaseEntityMgrImpl<TableTag> implements TableTagEntityMgr {

    @Autowired
    private TableTagDao tableTagDao;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Override
    public BaseDao<TableTag> getDao() {
        return tableTagDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<Table> getTablesForTag(String tagName) {
        List<TableTag> tags = tableTagDao.findAll();
        List<Table> tables = tags.stream()
                .filter(x -> x.getName().equals(tagName) && x.getTable().getTableType() == TableType.DATATABLE) //
                .map(x -> x.getTable()) //
                .map(x -> tableEntityMgr.findByName(x.getName())) //
                .collect(Collectors.toList());
        return tables;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<TableTag> getTableTagsForName(String tagName) {
        List<TableTag> tags = tableTagDao.findAll();
        return tags.stream().filter(x -> x.getName().equals(tagName) && x.getTable().getTableType() == TableType.DATATABLE) //
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void tagTable(Table table, String tagName) {
        TableTag tableTag = new TableTag();
        tableTag.setTable(table);
        tableTag.setName(tagName);
        tableTag.setTenantId(MultiTenantContext.getTenant().getPid());
        tableTagDao.create(tableTag);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void untagTable(String tableName, String tagName) {
        List<TableTag> tags = getTableTagsForName(tagName);
        for (TableTag tag: tags) {
            if (tableName.equals(tag.getTable().getName())) {
                tableTagDao.delete(tag);
            }
        }
    }

}
