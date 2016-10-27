package com.latticeengines.datacloud.core.entitymgr;

import java.util.Date;
import java.util.List;

import com.latticeengines.datacloud.core.source.IngestedRawSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.metadata.Table;

public interface HdfsSourceEntityMgr {

    String getCurrentVersion(Source source);

    void setCurrentVersion(Source source, String version);

    void setLatestTimestamp(IngestedRawSource source, Date timestamp);

    Date getLatestTimestamp(IngestedRawSource source);

    Table getTableAtVersion(Source source, String version);

    public Table getTableAtVersions(Source source, List<String> versions);

    Table getCollectedTableSince(IngestedRawSource source, Date earliest);

    Long count(Source source, String version);

    void purgeSourceAtVersion(Source source, String version);

    List<String> getVersions(Source source);

}
