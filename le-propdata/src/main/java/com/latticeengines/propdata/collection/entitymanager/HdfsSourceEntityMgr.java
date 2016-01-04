package com.latticeengines.propdata.collection.entitymanager;


import java.util.Date;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.collection.source.CollectedSource;
import com.latticeengines.propdata.collection.source.Source;

public interface HdfsSourceEntityMgr {

    String getCurrentVersion(Source source);

    void setCurrentVersion(Source source, String version);

    void setLatestTimestamp(CollectedSource source, Date timestamp);

    Date getLatestTimestamp(CollectedSource source);

    Table getTableAtVersion(Source source, String version);

    Table getCollectedTableSince(CollectedSource source, Date earliest);

}
