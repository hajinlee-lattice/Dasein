package com.latticeengines.datacloud.collection.service;

import com.latticeengines.datacloud.core.source.MostRecentSource;
import com.latticeengines.datacloud.core.source.PivotedSource;

public interface CollectionDataFlowService {

    void executeMergeRawData(MostRecentSource source, String uid, String flowBean);

    void executePivotData(PivotedSource source, String baseVersion, String uid, String flowBean);

    void executeRefreshHGData(String baseVersion, String uid);

}
