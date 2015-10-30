package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.pls.entitymanager.QuotaEntityMgr;
import com.latticeengines.pls.service.QuotaService;

@Component("quotaService")
public class QuotaServiceImpl implements QuotaService {

    @Autowired
    QuotaEntityMgr quotaEntityMgr;

    @Override
    public void create(Quota quota) {
        this.quotaEntityMgr.create(quota);
    }

    @Override
    public void deleteQuotaByQuotaId(String quotaId) {
        this.quotaEntityMgr.deleteQuotaByQuotaId(quotaId);
    }

    @Override
    public Quota findQuotaByQuotaId(String quotaId) {
        return this.quotaEntityMgr.findQuotaByQuotaId(quotaId);
    }

    @Override
    public void updateQuotaByQuotaId(Quota quota, String quotaId) {
        this.quotaEntityMgr.updateQuotaByQuotaId(quota, quotaId);
    }

}
