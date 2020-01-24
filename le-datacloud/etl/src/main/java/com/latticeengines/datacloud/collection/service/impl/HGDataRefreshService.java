package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.impl.HGData;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@Component("hgDataRefreshService")
public class HGDataRefreshService extends AbstractRefreshService implements RefreshService {

    @Inject
    private RefreshProgressEntityMgr progressEntityMgr;

    @Inject
    private HGData source;

    @Override
    public String getBeanName() {
        return "hgDataRefreshService";
    }

    @Override
    public DerivedSource getSource() { return source; }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

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
