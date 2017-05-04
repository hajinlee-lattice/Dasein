package com.latticeengines.eai.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.eai.entitymanager.EaiImportJobDetailEntityMgr;
import com.latticeengines.eai.service.EaiImportJobDetailService;

@Component("eaiImportJobDetailService")
public class EaiImportJobDetailServiceImpl implements EaiImportJobDetailService {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private EaiImportJobDetailEntityMgr eaiImportJobDetailEntityMgr;

    @Override
    public EaiImportJobDetail getImportJobDetail(String collectionIdentifier) {
        return eaiImportJobDetailEntityMgr.findByCollectionIdentifier(collectionIdentifier);
    }

    @Override
    public boolean updateImportJobDetail(EaiImportJobDetail eaiImportJobDetail) {
        eaiImportJobDetailEntityMgr.update(eaiImportJobDetail);
        return true;
    }

    @Override
    public void createImportJobDetail(EaiImportJobDetail eaiImportJobDetail) {
        eaiImportJobDetailEntityMgr.create(eaiImportJobDetail);
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
                YarnUtils.kill(yarnConfiguration,
                        ConverterUtils.toApplicationId(eaiImportJobDetail.getLoadApplicationId()));
            }
        }
    }
}
