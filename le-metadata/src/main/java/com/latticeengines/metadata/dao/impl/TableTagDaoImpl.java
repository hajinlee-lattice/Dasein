package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.TableTag;
import com.latticeengines.metadata.dao.TableTagDao;

@Component("tableTagDao")
public class TableTagDaoImpl extends BaseDaoImpl<TableTag> implements TableTagDao {

    @Override
    protected Class<TableTag> getEntityClass() {
        return TableTag.class;
    }

}
