package com.latticeengines.datacloud.etl.orchestration.entitymgr.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.orchestration.dao.OrchestrationProgressDao;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationProgressEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;

@Component("orchestrationProgressEntityMgr")
public class OrchestrationProgressEntityMgrImpl implements OrchestrationProgressEntityMgr {
    @Autowired
    private OrchestrationProgressDao orchestrationProgressDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public List<OrchestrationProgress> findProgressesByField(Map<String, Object> fields, List<String> orderFields) {
        return orchestrationProgressDao.findProgressesByField(fields, orderFields);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public OrchestrationProgress findProgress(OrchestrationProgress progress) {
        return orchestrationProgressDao.findByKey(progress);
    }

    @Override
    public OrchestrationProgress saveProgress(OrchestrationProgress progress) {
        String podIdInProgress = progress.getHdfsPod();
        String podIdInContext = HdfsPodContext.getHdfsPodId();
        if (StringUtils.isNotBlank(podIdInProgress) && !podIdInProgress.equals(podIdInContext)) {
            throw new IllegalArgumentException("You are in the pod " + podIdInContext
                    + ", but you are trying to update/create a progress in the pod " + podIdInProgress);
        } else if (StringUtils.isBlank(podIdInProgress)) {
            progress.setHdfsPod(podIdInContext);
        }
        orchestrationProgressDao.createOrUpdate(progress);
        return orchestrationProgressDao.findByKey(progress);
    }

    @Override
    public void saveProgresses(List<OrchestrationProgress> progresses) {
        for (OrchestrationProgress progress : progresses) {
            saveProgress(progress);
        }
    }

    @Override
    public List<OrchestrationProgress> findProgressesToKickoff() {
        return orchestrationProgressDao.findProgressesToKickoff();
    }

}
