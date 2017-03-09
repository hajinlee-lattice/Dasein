package com.latticeengines.metadata.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.VdbImportExtract;
import com.latticeengines.metadata.dao.VdbImportExtractDao;
import org.springframework.stereotype.Component;

@Component("vdbImportExtractDao")
public class VdbImportExtractDaoImpl extends BaseDaoImpl<VdbImportExtract> implements VdbImportExtractDao {
    @Override
    protected Class<VdbImportExtract> getEntityClass() {
        return VdbImportExtract.class;
    }
}
