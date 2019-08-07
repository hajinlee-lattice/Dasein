package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.ConvertBatchStoreInfoEntityMgr;
import com.latticeengines.apps.cdl.service.ConvertBatchStoreInfoService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreDetail;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("convertBatchStoreInfoService")
public class ConvertBatchStoreInfoServiceImpl implements ConvertBatchStoreInfoService {

    @Inject
    private ConvertBatchStoreInfoEntityMgr convertBatchStoreInfoEntityMgr;

    @Override
    public ConvertBatchStoreInfo create(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        ConvertBatchStoreInfo convertBatchStoreInfo = new ConvertBatchStoreInfo();
        convertBatchStoreInfo.setTenant(tenant);
        convertBatchStoreInfoEntityMgr.create(convertBatchStoreInfo);
        return convertBatchStoreInfo;
    }

    @Override
    public ConvertBatchStoreInfo getByPid(String customerSpace, Long pid) {
        return convertBatchStoreInfoEntityMgr.findByPid(pid);
    }

    @Override
    public void updateDetails(String customerSpace, Long pid, ConvertBatchStoreDetail convertDetail) {
        ConvertBatchStoreInfo convertBatchStoreInfo = convertBatchStoreInfoEntityMgr.findByPid(pid);
        if (convertBatchStoreInfo == null) {
            throw new RuntimeException("Cannot find ConvertBatchStoreInfo with Pid: " + pid);
        }
        convertBatchStoreInfo.setConvertDetail(convertDetail);
        convertBatchStoreInfoEntityMgr.update(convertBatchStoreInfo);
    }
}
