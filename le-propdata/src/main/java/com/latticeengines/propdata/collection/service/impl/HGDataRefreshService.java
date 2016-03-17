package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.impl.HGData;

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
    public DerivedSource getSource() { return source; }

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
    protected void createStageTable() {
        String sql = "CREATE TABLE [" + getStageTableName() + "](\n" +
                "\t[Domain] [nvarchar](255) NOT NULL,\n" +
                "\t[Supplier_Name] [nvarchar](255) NULL,\n" +
                "\t[Segment_Name] [nvarchar](255) NULL,\n" +
                "\t[HG_Category_1] [nvarchar](255) NULL,\n" +
                "\t[HG_Category_2] [nvarchar](255) NULL,\n" +
                "\t[HG_Category_1_Parent] [nvarchar](255) NULL,\n" +
                "\t[HG_Category_2_Parent] [nvarchar](255) NULL,\n" +
                "\t[Creation_Date] [DATETIME] NULL,\n" +
                "\t[Last_Verified_Date] [DATETIME] NULL,\n" +
                "\t[LE_Last_Upload_Date] [DATETIME] NULL,\n" +
                "\t[Location_Count] [INT] NULL,\n" +
                "\t[Max_Location_Intensity] [INT] NULL)";
        jdbcTemplateCollectionDB.execute(sql);
    }

    @Override
    public String findBaseVersionForNewProgress() {
        String currentVersion = hdfsSourceEntityMgr.getCurrentVersion(getSource().getBaseSources()[0]);
        if (getProgressEntityMgr().findProgressByBaseVersion(getSource(), currentVersion) == null) {
            return currentVersion;
        }
        return null;
    }
}
