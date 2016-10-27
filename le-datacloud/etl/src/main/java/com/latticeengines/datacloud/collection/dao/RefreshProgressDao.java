package com.latticeengines.datacloud.collection.dao;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

public interface RefreshProgressDao extends ProgressDao<RefreshProgress> {

    RefreshProgress findByBaseSourceVersion(DerivedSource source, String baseSourceVersion);

}
