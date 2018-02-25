package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;

public interface IsColumnMetadata extends Serializable {

    ColumnMetadata toColumnMetadata();

}
