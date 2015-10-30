package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.Quota;

public interface QuotaEntityMgr extends BaseEntityMgr<Quota> {

    List<Quota> getAllQuotas();

    Quota findQuotaByQuotaId(String quotaId);

    void deleteQuotaByQuotaId(String quotaId);

    void updateQuotaByQuotaId(Quota quota, String quotaId);
}
