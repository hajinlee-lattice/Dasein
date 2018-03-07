package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataFeedImportDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;

@Component("datafeedImportDao")
public class DataFeedImportDaoImpl extends BaseDaoImpl<DataFeedImport> implements DataFeedImportDao {

    @Override
    protected Class<DataFeedImport> getEntityClass() {
        return DataFeedImport.class;
    }

}
