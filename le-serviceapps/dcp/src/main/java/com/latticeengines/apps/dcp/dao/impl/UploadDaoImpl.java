package com.latticeengines.apps.dcp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.UploadDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dcp.Upload;

@Component("uploadDao")
public class UploadDaoImpl extends BaseDaoImpl<Upload> implements UploadDao {

    @Override
    protected Class<Upload> getEntityClass() {
        return Upload.class;
    }
}
