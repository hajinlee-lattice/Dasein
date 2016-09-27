package com.latticeengines.propdata.collection.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeysDeprecated;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.core.source.PivotedSource;
import com.latticeengines.propdata.core.source.Source;

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
