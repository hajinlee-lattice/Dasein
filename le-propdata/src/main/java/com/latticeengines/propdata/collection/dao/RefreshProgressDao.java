package com.latticeengines.propdata.collection.dao;

import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.core.source.ServingSource;

public interface RefreshProgressDao extends ProgressDao<RefreshProgress> {

    RefreshProgress findByBaseSourceVersion(ServingSource source, String baseSourceVersion);

}
