package com.latticeengines.dataplatform.dao.impl;

import com.latticeengines.dataplatform.dao.ModelDownloadFlagDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;
import org.springframework.stereotype.Component;

@Component("modelDownloadFlagDao")
public class ModelDownloadFlagDaoImpl extends BaseDaoImpl<ModelSummaryDownloadFlag> implements ModelDownloadFlagDao {
    @Override
    protected Class<ModelSummaryDownloadFlag> getEntityClass() {
        return ModelSummaryDownloadFlag.class;
    }
}
