package com.latticeengines.propdata.core.entitymgr;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.Source;

public interface HdfsSourceEntityMgr {

    String getCurrentVersion(Source source);

    void setCurrentVersion(Source source, String version);

    void setLatestTimestamp(CollectedSource source, Date timestamp);

    Date getLatestTimestamp(CollectedSource source);

    Table getTableAtVersion(Source source, String version);

    Table getCollectedTableSince(CollectedSource source, Date earliest);

    Long count(Source source, String version);

    void purgeSourceAtVersion(Source source, String version);

    List<String> getVersions(Source source);

}
