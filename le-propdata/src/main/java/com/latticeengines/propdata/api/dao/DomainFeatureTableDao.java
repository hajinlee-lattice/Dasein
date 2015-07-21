package com.latticeengines.propdata.api.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.DomainFeatureTable;

public interface DomainFeatureTableDao extends BaseDao<DomainFeatureTable> {

    DomainFeatureTable findByLookupID(String lookupID);
    
}
