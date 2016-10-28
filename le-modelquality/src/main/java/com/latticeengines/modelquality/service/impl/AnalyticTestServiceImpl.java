package com.latticeengines.modelquality.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.Environment;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.modelquality.entitymgr.AnalyticPipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.AnalyticTestEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.service.AnalyticTestService;
import com.latticeengines.modelquality.service.ModelRunService;

@Component("analyticTestService")
public class AnalyticTestServiceImpl extends BaseServiceImpl implements AnalyticTestService {

    @Autowired
    private AnalyticPipelineEntityMgr analyticPipelineEntityMgr;

    @Autowired
    private DataSetEntityMgr dataSetEntityMgr;

    @Autowired
    private AnalyticTestEntityMgr analyticTestEntityMgr;

    @Autowired
    private ModelRunService modelRunService;

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

        analyticTest.setAnalyticTestType(analyticTestEntityNames.getAnalyticTestType());
        
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
                    throw new LedpException(LedpCode.LEDP_35000, new String[] { "Analytic pipeline", analyticPipelineName});
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
                    throw new LedpException(LedpCode.LEDP_35000, new String[] { "Dataset", datasetName});
                }
            }
            analyticTest.setDataSets(dataSets);
        }

        analyticTestEntityMgr.create(analyticTest);

        return analyticTest;
    }

    @Override
    public List<String> executeByName(String analyticTestName) {
        ArrayList<String> resultSet = new ArrayList<String>();
        AnalyticTest analyticTest = analyticTestEntityMgr.findByName(analyticTestName);
        if (analyticTest == null) {
            throw new LedpException(LedpCode.LEDP_35000, new String[] { "Analytic Test", analyticTestName });
        }
        for (AnalyticPipeline ap : analyticTest.getAnalyticPipelines()) {
            for (DataSet ds : analyticTest.getDataSets()) {
                ModelRunEntityNames modelRunEntityNames = new ModelRunEntityNames();
                modelRunEntityNames.setAnalyticPipelineName(ap.getName());
                modelRunEntityNames.setDataSetName(ds.getName());
                modelRunEntityNames.setName(analyticTestName + "_" + ap.getPid() + "_" + ds.getPid() + "_" + UUID.randomUUID());
                modelRunEntityNames.setDescription("ModelRun created by the Execute Analytic Test API");
                modelRunEntityNames.setAnalyticTestTag(analyticTest.getAnalyticTestType().toString());
                ModelRun modelRun = modelRunService.createModelRun(modelRunEntityNames,
                        Environment.getCurrentEnvironment());
                resultSet.add(modelRun.getName());
            }
        }
        return resultSet;
    }

    @Transactional
    public AnalyticTestEntityNames getByName(String analyticTestName) {
        AnalyticTest atest = analyticTestEntityMgr.findByName(analyticTestName);
        AnalyticTestEntityNames result = new AnalyticTestEntityNames();
        result.setName(atest.getName());
        result.setPropDataMatchType(atest.getPropDataMatchType());
        result.setAnalyticTestType(atest.getAnalyticTestType());

        for (AnalyticPipeline ap : atest.getAnalyticPipelines()) {
            result.getAnalyticPipelineNames().add(ap.getName());
        }
        
        for (DataSet ds : atest.getDataSets()) {
            result.getDataSetNames().add(ds.getName());
        }
        return result;
    }
    
    @Transactional
    public List<AnalyticTestEntityNames> getAll() {

        List<AnalyticTestEntityNames> result = new ArrayList<>();
        for (AnalyticTest atest : analyticTestEntityMgr.findAll()) {
            AnalyticTestEntityNames atestName = new AnalyticTestEntityNames();
            atestName.setName(atest.getName());
            atestName.setPropDataMatchType(atest.getPropDataMatchType());
            atestName.setAnalyticTestType(atest.getAnalyticTestType());
            for (AnalyticPipeline ap : atest.getAnalyticPipelines()) {
                atestName.getAnalyticPipelineNames().add(ap.getName());
            }
            for (DataSet ds : atest.getDataSets()) {
                atestName.getDataSetNames().add(ds.getName());
            }

            result.add(atestName);
        }
        return result;
    }
}
