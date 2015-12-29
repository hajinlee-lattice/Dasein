package com.latticeengines.propdata.collection.source.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.BulkSource;
import com.latticeengines.propdata.collection.source.StageServer;

@Component("hgDataCustomers")
public class HGDataCustomers implements BulkSource {

    private static final long serialVersionUID = -1724598948350731339L;

    @Override
    public String getSourceName() {  return "HGDataCustomers"; }

    @Override
    public String getRefreshServiceBean() {  return "hgDataCustomersArchiveService"; }

    @Override
    public String getBulkStageTableName() { return "HG_Data_Customers"; }

    @Override
    public StageServer getBulkStageServer()  {  return StageServer.COLLECTION_DB; }

    @Override
    public String getDownloadSplitColumn() { return "Date Last Verified"; }

    @Override
    public String getTimestampField() { return "Date Last Verified"; }

}
