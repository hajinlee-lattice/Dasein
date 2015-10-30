package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.Quota;

public interface QuotaService {

    void create(Quota quota);

    Quota findQuotaByQuotaId(String quotaId);

    void deleteQuotaByQuotaId(String quotaId);

    void updateQuotaByQuotaId(Quota quota, String quotaId);

}
