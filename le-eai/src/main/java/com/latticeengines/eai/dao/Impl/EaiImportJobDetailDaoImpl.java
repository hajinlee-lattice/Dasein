package com.latticeengines.eai.dao.Impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.eai.dao.EaiImportJobDetailDao;
import org.springframework.stereotype.Component;

@Component("eaiImportJobDetailDao")
public class EaiImportJobDetailDaoImpl extends BaseDaoImpl<EaiImportJobDetail> implements EaiImportJobDetailDao {
    @Override
    protected Class<EaiImportJobDetail> getEntityClass() {
        return EaiImportJobDetail.class;
    }
}
