package com.latticeengines.propdata.collection.dao;

import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.propdata.collection.source.impl.PivotedSource;

public interface PivotProgressDao extends ProgressDao<PivotProgress> {

    PivotProgress findByBaseSourceVersion(PivotedSource source, String baseSourceVersion);

}
