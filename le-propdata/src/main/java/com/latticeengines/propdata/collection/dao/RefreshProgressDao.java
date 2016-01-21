package com.latticeengines.propdata.collection.dao;

import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.core.source.DerivedSource;

public interface RefreshProgressDao extends ProgressDao<RefreshProgress> {

    RefreshProgress findByBaseSourceVersion(DerivedSource source, String baseSourceVersion);

}
