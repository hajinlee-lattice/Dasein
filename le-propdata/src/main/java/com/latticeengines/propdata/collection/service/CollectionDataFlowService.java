package com.latticeengines.propdata.collection.service;

import com.latticeengines.propdata.collection.source.MostRecentSource;
import com.latticeengines.propdata.collection.source.PivotedSource;

public interface CollectionDataFlowService {

    void executeMergeRawData(MostRecentSource source, String uid) ;

    void executePivotData(PivotedSource source, String baseVersion, String uid);

    void executeRefreshHGData(String baseVersion, String uid);

}
