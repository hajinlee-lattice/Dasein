package com.latticeengines.propdata.collection.source;

public interface ServingSource extends Source {

    String getSqlTableName();

    Source[] getBaseSources();
}
