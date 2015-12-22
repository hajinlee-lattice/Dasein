package com.latticeengines.propdata.collection.source;

public interface Source {

    String getSourceName();

    String getSqlTableName();

    String getRefreshServiceBean();

}
