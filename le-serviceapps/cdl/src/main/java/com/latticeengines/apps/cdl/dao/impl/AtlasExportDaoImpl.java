package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.AtlasExportDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.AtlasExport;

@Component("atlasExportDao")
public class AtlasExportDaoImpl extends BaseDaoImpl<AtlasExport> implements AtlasExportDao {
    @Override
    protected Class<AtlasExport> getEntityClass() {
        return AtlasExport.class;
    }
}
