package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ConvertBatchStoreInfoDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;

@Component("convertBatchStoreInfoDao")
public class ConvertBatchStoreInfoDaoImpl extends BaseDaoImpl<ConvertBatchStoreInfo> implements ConvertBatchStoreInfoDao {
    @Override
    protected Class<ConvertBatchStoreInfo> getEntityClass() {
        return ConvertBatchStoreInfo.class;
    }
}
