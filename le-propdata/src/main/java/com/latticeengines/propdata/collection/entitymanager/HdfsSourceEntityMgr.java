package com.latticeengines.propdata.collection.entitymanager;

import com.latticeengines.propdata.collection.source.Source;

public interface HdfsSourceEntityMgr {

    String getCurrentVersion(Source source);

    void setCurrentVersion(Source source, String version);

}
