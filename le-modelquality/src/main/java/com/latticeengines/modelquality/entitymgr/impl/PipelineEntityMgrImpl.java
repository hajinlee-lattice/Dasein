package com.latticeengines.modelquality.entitymgr.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyDef;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyValue;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.modelquality.dao.PipelineDao;
import com.latticeengines.modelquality.dao.PipelineStepDao;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;

@Component("pipelineEntityMgr")
public class PipelineEntityMgrImpl extends BaseEntityMgrImpl<Pipeline> implements PipelineEntityMgr {

    @Autowired
    private PipelineDao pipelineDao;

    @Autowired
    private PipelineStepDao pipelineStepEntityDao;

    @Override
    public BaseDao<Pipeline> getDao() {
        return pipelineDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deletePipelines(List<Pipeline> pipelines) {
        for (Pipeline oldPipeline : pipelines) {
            pipelineDao.delete(oldPipeline);
        }
        pipelineDao.deleteAll();
        pipelineStepEntityDao.deleteAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createPipelines(List<Pipeline> pipelines) {
        Map<String, PipelineStep> pipelineMap = new HashMap<>();
        for (Pipeline pipeline : pipelines) {
            setupPipeline(pipeline, pipelineMap);
            pipelineDao.createOrUpdate(pipeline);
        }
    }

    private void setupPipeline(Pipeline pipeline, Map<String, PipelineStep> pipelineMap) {
        List<PipelineStep> pipelineSteps = pipeline.getPipelineSteps();
        if (pipelineSteps != null) {
            for (PipelineStep pipelineStep : pipelineSteps) {
                List<PipelinePropertyDef> defs = pipelineStep.getPipelinePropertyDefs();
                if (defs != null) {
                    for (PipelinePropertyDef def : defs) {
                        List<PipelinePropertyValue> values = def.getPipelinePropertyValues();
                        if (values != null) {
                            for (PipelinePropertyValue value : values) {
                                value.setPipelinePropertyDef(def);
                            }
                        }
                        def.setPipelineStep(pipelineStep);
                    }
                }

                if (!pipelineMap.containsKey(pipelineStep.getName())) {
                    pipelineStep.addPipeline(pipeline);
                    pipelineMap.put(pipelineStep.getName(), pipelineStep);
                } else {
                    PipelineStep existingStep = pipelineMap.get(pipelineStep.getName());
                    existingStep.addPipeline(pipeline);
                    setupPipeLineStep(pipeline, existingStep);
                }
            }
        }
    }

    private void setupPipeLineStep(Pipeline pipeline, PipelineStep existingStep) {
        List<PipelineStep> pipelineSteps = pipeline.getPipelineSteps();
        int i = 0;
        for (PipelineStep step : pipelineSteps) {
            if (step.getName().equals(existingStep.getName())) {
                break;
            }
            i++;
        }
        pipelineSteps.set(i, existingStep);
    }

}
