package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataCollectionArtifactDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;

@Component("dataCollectionArtifactDao")
public class DataCollectionArtifactDaoImpl extends BaseDaoImpl<DataCollectionArtifact>
        implements DataCollectionArtifactDao {

    @Override
    protected Class<DataCollectionArtifact> getEntityClass() {
        return DataCollectionArtifact.class;
    }
}
