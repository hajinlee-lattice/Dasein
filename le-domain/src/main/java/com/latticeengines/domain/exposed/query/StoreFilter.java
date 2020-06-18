package com.latticeengines.domain.exposed.query;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

public enum StoreFilter {
    ALL, NON_LDC, DATE_ATTR;

    public boolean accept(ColumnMetadata cm) {
        switch (this) {
            case DATE_ATTR:
                return cm.isDateAttribute();
            default:
                return true;
        }
    }
}
