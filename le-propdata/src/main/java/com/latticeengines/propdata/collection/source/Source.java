package com.latticeengines.propdata.collection.source;

import java.io.Serializable;

public interface Source extends Serializable {

    String getSourceName();

    String getRefreshServiceBean();

    String getTimestampField();

    String[] getPrimaryKey();

}
