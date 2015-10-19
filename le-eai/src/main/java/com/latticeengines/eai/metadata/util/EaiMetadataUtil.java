package com.latticeengines.eai.metadata.util;

import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;

public class EaiMetadataUtil {

    public static PrimaryKey createPrimaryKey() {
        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.setDisplayName("Primary key for the identity column(s)");
        return pk;
    }

    public static LastModifiedKey createLastModifiedKey() {
        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_LUD");
        lk.setDisplayName("Last Modified Key to hold column containing last modified timestamp");
        return lk;
    }
}
