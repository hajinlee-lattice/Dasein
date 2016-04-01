package com.latticeengines.propdata.core.source;

import java.io.Serializable;

public interface Source extends Serializable {

    String getSourceName();

    String getTimestampField();

    String[] getPrimaryKey();

    String getDefaultCronExpression();

}
