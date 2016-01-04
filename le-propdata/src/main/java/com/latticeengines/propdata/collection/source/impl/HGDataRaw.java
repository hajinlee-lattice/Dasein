package com.latticeengines.propdata.collection.source.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.BulkSource;
import com.latticeengines.propdata.collection.source.StageServer;

@Component("hgDataCustomers")
public class HGDataRaw implements BulkSource {

    private static final long serialVersionUID = -1724598948350731339L;

    @Value("${propdata.job.hgdataraw.schedule:}")
    String cronExpression;

    @Override
    public String getSourceName() {  return "HGDataRaw"; }

    @Override
    public String getRefreshServiceBean() {  return "hgDataRawArchiveService"; }

    @Override
    public String getBulkStageTableName() { return "HG_Data_Customers"; }

    @Override
    public StageServer getBulkStageServer()  {  return StageServer.COLLECTION_DB; }

    @Override
    public String getDownloadSplitColumn() { return "Date Last Verified"; }

    @Override
    public String getTimestampField() { return "Date Last Verified"; }

    @Override
    public String[] getPrimaryKey() { return null;  }

    @Override
    public String getCronExpression() { return cronExpression; }

}
