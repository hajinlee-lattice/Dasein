package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.TableRelationship;
import com.latticeengines.metadata.dao.TableRelationshipDao;

@Component("tableRelationshipDao")
public class TableRelationshipDaoImpl extends BaseDaoImpl<TableRelationship> implements TableRelationshipDao {
    @Override
    protected Class<TableRelationship> getEntityClass() {
        return TableRelationship.class;
    }
}
