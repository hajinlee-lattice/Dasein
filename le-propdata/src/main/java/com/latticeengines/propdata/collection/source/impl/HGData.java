package com.latticeengines.propdata.collection.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.source.DomainBased;
import com.latticeengines.propdata.collection.source.ServingSource;
import com.latticeengines.propdata.collection.source.Source;

@Component("hgData")
public class HGData implements DomainBased, ServingSource {

    private static final long serialVersionUID = 603829385601451984L;

    @Autowired
    HGDataRaw baseSource;

    @Override
    public String getSourceName() { return "HGData"; }

    @Override
    public String getSqlTableName() { return "HGData_Source"; }

    @Override
    public String getRefreshServiceBean() { return "hgDataRefreshService"; }

    @Override
    public String[] getPrimaryKey() {
        return new String[]{"URL", "Supplier_Name", "Segment_Name",
                "HG_Category_1", "HG_Category_2", "HG_Category_1_Parent", "HG_Category_2_Parent"};
    }

    @Override
    public String getTimestampField() { return "LE_Last_Upload_Date"; }

    @Override
    public String getDomainField() {  return "URL"; }

    @Override
    public Source[] getBaseSources() { return new Source[]{ baseSource }; }

    @Override
    public String getCronExpression() { return ""; }

}