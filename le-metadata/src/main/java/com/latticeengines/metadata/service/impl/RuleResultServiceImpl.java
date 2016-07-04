package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelreview.BaseRuleResult;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.ColumnRuleResultEntityMgr;
import com.latticeengines.metadata.entitymgr.RowRuleResultEntityMgr;
import com.latticeengines.metadata.service.RuleResultService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("ruleResultService")
public class RuleResultServiceImpl implements RuleResultService {

    @Autowired
    private ColumnRuleResultEntityMgr columnRuleResultEntityMgr;

    @Autowired
    private RowRuleResultEntityMgr rowRuleResultEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public void createColumnResults(List<ColumnRuleResult> columnResults) {
        checkAndSetTenantOnRuleResults(columnResults);
        for (ColumnRuleResult columnRuleResult : columnResults) {
            columnRuleResultEntityMgr.create(columnRuleResult);
        }
    }

    @Override
    public void createRowResults(List<RowRuleResult> rowResults) {
        checkAndSetTenantOnRuleResults(rowResults);
        for (RowRuleResult rowRuleResult : rowResults) {
            rowRuleResultEntityMgr.create(rowRuleResult);
        }
    }

    private void checkAndSetTenantOnRuleResults(List<? extends BaseRuleResult> results) {
        for (BaseRuleResult result : results) {
            Tenant tenant = result.getTenant();
            if (tenant.getPid() == null) {
                Tenant retrievedTenant = tenantEntityMgr.findByTenantId(tenant.getId());
                result.setTenant(retrievedTenant);
            }
        }
    }

    @Override
    public List<ColumnRuleResult> findColumnResults(String modelId) {
        return columnRuleResultEntityMgr.findByModelId(modelId);
    }

    @Override
    public List<RowRuleResult> findRowResults(String modelId) {
        return rowRuleResultEntityMgr.findByModelId(modelId);
    }

}
