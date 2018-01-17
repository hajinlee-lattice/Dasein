package com.latticeengines.eai.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.eai.entitymanager.EaiImportJobDetailEntityMgr;
import com.latticeengines.eai.service.EaiImportJobDetailService;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("eaiImportJobDetailService")
public class EaiImportJobDetailServiceImpl implements EaiImportJobDetailService {

    @SuppressWarnings("unused")
    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JobService jobService;

    @Autowired
    private EaiImportJobDetailEntityMgr eaiImportJobDetailEntityMgr;

    @Override
    public EaiImportJobDetail getImportJobDetailByCollectionIdentifier(String collectionIdentifier) {
        return eaiImportJobDetailEntityMgr.findByCollectionIdentifier(collectionIdentifier);
    }

    @Override
    public EaiImportJobDetail getImportJobDetailByAppId(String appId) {
        return eaiImportJobDetailEntityMgr.findByApplicationId(appId);
    }

    @Override
    public EaiImportJobDetail getImportJobDetailById(Long id) {
        return eaiImportJobDetailEntityMgr.findByField("PID", id);
    }

    @Override
    public boolean updateImportJobDetail(EaiImportJobDetail eaiImportJobDetail) {
        eaiImportJobDetailEntityMgr.update(eaiImportJobDetail);
        return true;
    }

    @Override
    public void createImportJobDetail(EaiImportJobDetail eaiImportJobDetail) {
        eaiImportJobDetailEntityMgr.createImportJobDetail(eaiImportJobDetail);
    }

    @Override
    public void deleteImportJobDetail(EaiImportJobDetail eaiImportJobDetail) {
        eaiImportJobDetailEntityMgr.delete(eaiImportJobDetail);
    }

    @Override
    public void cancelImportJob(String collectionIdentifier) {
        EaiImportJobDetail eaiImportJobDetail = eaiImportJobDetailEntityMgr
                .findByCollectionIdentifier(collectionIdentifier);
        if (eaiImportJobDetail != null) {
            if (!StringUtils.isEmpty(eaiImportJobDetail.getLoadApplicationId())) {
                // YarnUtils.kill(client,ConverterUtils.toApplicationId(eaiImportJobDetail.getLoadApplicationId()));
                jobService.killJob(ConverterUtils.toApplicationId(eaiImportJobDetail.getLoadApplicationId()));
            }
        }
    }
}
