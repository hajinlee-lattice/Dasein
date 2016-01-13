package com.latticeengines.propdata.core.source;

public interface ServingSource extends Source {

    String getSqlTableName();

    Source[] getBaseSources();
}
