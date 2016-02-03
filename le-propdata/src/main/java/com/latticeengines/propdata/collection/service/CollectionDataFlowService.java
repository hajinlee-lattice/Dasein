package com.latticeengines.propdata.collection.service;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.source.MostRecentSource;
import com.latticeengines.propdata.core.source.PivotedSource;

public interface CollectionDataFlowService {

    void executeMergeRawData(MostRecentSource source, String uid);

    void executePivotData(PivotedSource source, String baseVersion, String uid, String flowBean);

    void executeRefreshHGData(String baseVersion, String uid);

    void executeRefreshOrbIntelligence(String uid);

    Long executeCountFlow(Table sourceTable);

}
