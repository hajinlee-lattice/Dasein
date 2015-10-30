package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Quota;

public interface QuotaDao extends BaseDao<Quota> {

    Quota findQuotaByQuotaId(String quotaId);

}
