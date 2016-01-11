package com.latticeengines.propdata.collection.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.Source;

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
        return hdfsPathBuilder.constructWorkFlowDir(getSource(), CollectionDataFlowKeys.PIVOT_FLOW)
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
