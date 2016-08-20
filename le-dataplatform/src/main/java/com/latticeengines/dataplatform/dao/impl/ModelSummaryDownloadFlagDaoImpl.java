package com.latticeengines.dataplatform.dao.impl;

import com.latticeengines.dataplatform.dao.ModelSummaryDownloadFlagDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;
import org.springframework.stereotype.Component;

@Component("modelSummaryDownloadFlagDao")
public class ModelSummaryDownloadFlagDaoImpl extends BaseDaoImpl<ModelSummaryDownloadFlag> implements ModelSummaryDownloadFlagDao {
    @Override
    protected Class<ModelSummaryDownloadFlag> getEntityClass() {
        return ModelSummaryDownloadFlag.class;
    }
}
