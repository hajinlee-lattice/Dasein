package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.dao.TableDao;

@Component("tableDao")
public class TableDaoImpl extends BaseDaoImpl<Table> implements TableDao {

    @Override
    protected Class<Table> getEntityClass() {
        return Table.class;
    }

}
