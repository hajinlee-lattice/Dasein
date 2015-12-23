package com.latticeengines.propdata.collection.source;

import java.io.Serializable;

public interface Source extends Serializable {

    String getSourceName();

    String getSqlTableName();

    String getRefreshServiceBean();

    String[] getPrimaryKey();

    String getTimestampField(); // every source needs to be timestamped

}
