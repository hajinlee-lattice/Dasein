package com.latticeengines.modelquality.service.impl;

import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.modelquality.entitymgr.AnalyticPipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.AnalyticTestEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.service.AnalyticTestService;

@Component("analyticTestService")
public class AnalyticTestServiceImpl extends BaseServiceImpl implements AnalyticTestService {

    @Autowired
    private AnalyticPipelineEntityMgr analyticPipelineEntityMgr;

    @Autowired
    private DataSetEntityMgr dataSetEntityMgr;

    @Autowired
    private AnalyticTestEntityMgr analyticTestEntityMgr;

    @Override
    public AnalyticTest createAnalyticTest(AnalyticTestEntityNames analyticTestEntityNames) {

        AnalyticTest analyticTest = new AnalyticTest();
        if (analyticTestEntityNames.getName() == null || analyticTestEntityNames.getName().trim().isEmpty()) {
            throw new RuntimeException("AnalyticTest Name cannot be empty");
        }
        analyticTest.setName(analyticTestEntityNames.getName());

        if (analyticTestEntityNames.getPropDataMatchType() == null) {
            throw new RuntimeException("Need to specify a Propdata match type");
        }
        analyticTest.setPropDataMatchType(analyticTestEntityNames.getPropDataMatchType());

        if (analyticTestEntityNames.getAnalyticPipelineNames() == null
                || analyticTestEntityNames.getAnalyticPipelineNames().isEmpty()) {
            throw new RuntimeException("Need to provide atleast one AnalyticPipeline");
        } else {
            ArrayList<AnalyticPipeline> analyticPipelines = new ArrayList<AnalyticPipeline>();
            for (String analyticPipelineName : analyticTestEntityNames.getAnalyticPipelineNames()) {
                AnalyticPipeline ap = analyticPipelineEntityMgr.findByName(analyticPipelineName);
                if (ap != null) {
                    analyticPipelines.add(ap);
                } else {
                    throw new RuntimeException("No AnalyticPipeline with name " + analyticPipelineName + " not found");
                }
            }
            analyticTest.setAnalyticPipelines(analyticPipelines);
        }

        if (analyticTestEntityNames.getDataSetNames() == null || analyticTestEntityNames.getDataSetNames().isEmpty()) {
            throw new RuntimeException("Need to provide atleast one Dataset");
        } else {
            ArrayList<DataSet> dataSets = new ArrayList<DataSet>();
            for (String datasetName : analyticTestEntityNames.getDataSetNames()) {
                DataSet ds = dataSetEntityMgr.findByName(datasetName);
                if (ds != null) {
                    dataSets.add(ds);
                } else {
                    throw new RuntimeException("No Dataset with name " + datasetName + " not found");
                }
            }
            analyticTest.setDataSets(dataSets);
        }

        analyticTestEntityMgr.create(analyticTest);

        return analyticTest;
    }
}
