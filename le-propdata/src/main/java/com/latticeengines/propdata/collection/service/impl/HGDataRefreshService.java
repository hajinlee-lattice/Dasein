package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.collection.source.ServingSource;
import com.latticeengines.propdata.collection.source.impl.HGData;

@Component("hgDataRefreshService")
public class HGDataRefreshService extends AbstractRefreshService implements RefreshService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr archiveProgressEntityMgr;

    @Autowired
    RefreshProgressEntityMgr progressEntityMgr;

    @Autowired
    HGData source;

    @Override
    public ServingSource getSource() { return source; }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executeRefreshHGData(
                progress.getBaseSourceVersion(),
                progress.getRootOperationUID()
        );
    }

    @Override
    protected String createStageTableSql() {
        return "CREATE TABLE [" + getStageTableName() + "](\n" +
                "\t[Domain] [nvarchar](255) NOT NULL,\n" +
                "\t[Supplier_Name] [nvarchar](255) NULL,\n" +
                "\t[Segment_Name] [nvarchar](255) NULL,\n" +
                "\t[HG_Category_1] [nvarchar](255) NULL,\n" +
                "\t[HG_Category_2] [nvarchar](255) NULL,\n" +
                "\t[HG_Category_1_Parent] [nvarchar](255) NULL,\n" +
                "\t[HG_Category_2_Parent] [nvarchar](255) NULL,\n" +
                "\t[Creation_Date] [DATETIME] NULL,\n" +
                "\t[LE_Last_Upload_Date] [DATETIME] NULL,\n" +
                "\t[Location_Count] [INT] NULL,\n" +
                "\t[Max_Location_Intensity] [INT] NULL)\n" +
                "CREATE INDEX IX_DOMAIN ON [\" + getStageTableName() + \"] ([Domain] ASC)\n";
    }
}
