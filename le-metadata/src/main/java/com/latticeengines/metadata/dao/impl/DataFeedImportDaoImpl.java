package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.metadata.dao.DataFeedImportDao;

@Component("datafeedImportDao")
public class DataFeedImportDaoImpl extends BaseDaoImpl<DataFeedImport> implements DataFeedImportDao {

    @Override
    protected Class<DataFeedImport> getEntityClass() {
        return DataFeedImport.class;
    }

}
