package com.latticeengines.datacloud.collection.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.datacloud.collection.service.CollectionDataFlowKeysDeprecated;
import com.latticeengines.datacloud.collection.service.PivotService;
import com.latticeengines.datacloud.core.source.PivotedSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@SuppressWarnings("deprecation")
public abstract class AbstractPivotService extends AbstractRefreshService implements PivotService {

    @Override
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executePivotData(
                (PivotedSource) getSource(),
                progress.getBaseSourceVersion(),
                progress.getRootOperationUID(),
                null
        );
    }

    @Override
    protected String workflowDirInHdfs(RefreshProgress progress) {
        return hdfsPathBuilder.constructWorkFlowDir(getSource(), CollectionDataFlowKeysDeprecated.PIVOT_FLOW)
                .append(progress.getRootOperationUID()).toString();
    }

    @Override
    public String findBaseVersionForNewProgress() {
        List<String> currentVersions = new ArrayList<>();
        for (Source source: getSource().getBaseSources()) {
            currentVersions.add(hdfsSourceEntityMgr.getCurrentVersion(source));
        }
        String combinedCurrentVersion = StringUtils.join(currentVersions, "|");
        if (getProgressEntityMgr().findProgressByBaseVersion(getSource(), combinedCurrentVersion) == null) {
            return combinedCurrentVersion;
        }
        return null;
    }

}
