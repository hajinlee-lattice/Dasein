package com.latticeengines.modelquality.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;
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

    @Autowired
    private ModelRunEntityMgr modelRunEntityMgr;

    @Value("${common.pls.url}")
    private String apiHostPort;

    @Override
    public AnalyticTest createAnalyticTest(AnalyticTestEntityNames analyticTestEntityNames) {

        AnalyticTest analyticTest = new AnalyticTest();
        if (analyticTestEntityNames.getName() == null || analyticTestEntityNames.getName().trim().isEmpty()) {
            throw new LedpException(LedpCode.LEDP_35003, new String[] { "AnalyticTest Name" });
        }

        if (analyticTestEntityMgr.findByName(analyticTestEntityNames.getName()) != null) {
            throw new LedpException(LedpCode.LEDP_35002,
                    new String[] { "AnalyticTest", analyticTestEntityNames.getName() });
        }

        analyticTest.setName(analyticTestEntityNames.getName());

        if (analyticTestEntityNames.getPropDataMatchType() == null) {
            throw new LedpException(LedpCode.LEDP_35003, new String[] { "Propdata Match Type" });
        }
        analyticTest.setPropDataMatchType(analyticTestEntityNames.getPropDataMatchType());

        analyticTest.setAnalyticTestTag(analyticTestEntityNames.getAnalyticTestTag());

        if (analyticTestEntityNames.getAnalyticPipelineNames() == null
                || analyticTestEntityNames.getAnalyticPipelineNames().isEmpty()) {
            throw new LedpException(LedpCode.LEDP_35003, new String[] { "AnalyticPipelines" });
        } else {
            ArrayList<AnalyticPipeline> analyticPipelines = new ArrayList<AnalyticPipeline>();
            for (String analyticPipelineName : analyticTestEntityNames.getAnalyticPipelineNames()) {
                AnalyticPipeline ap = analyticPipelineEntityMgr.findByName(analyticPipelineName);
                if (ap != null) {
                    analyticPipelines.add(ap);
                } else {
                    throw new LedpException(LedpCode.LEDP_35000,
                            new String[] { "Analytic pipeline", analyticPipelineName });
                }
            }
            analyticTest.setAnalyticPipelines(analyticPipelines);
        }

        if (analyticTestEntityNames.getDataSetNames() == null || analyticTestEntityNames.getDataSetNames().isEmpty()) {
            throw new LedpException(LedpCode.LEDP_35003, new String[] { "Datasets" });
        } else {
            ArrayList<DataSet> dataSets = new ArrayList<DataSet>();
            for (String datasetName : analyticTestEntityNames.getDataSetNames()) {
                DataSet ds = dataSetEntityMgr.findByName(datasetName);
                if (ds != null) {
                    dataSets.add(ds);
                } else {
                    throw new LedpException(LedpCode.LEDP_35000, new String[] { "Dataset", datasetName });
                }
            }
            analyticTest.setDataSets(dataSets);
        }

        analyticTestEntityMgr.create(analyticTest);

        return analyticTest;
    }

    @Override
    public List<ModelRun> executeByName(String analyticTestName) {
        ArrayList<ModelRun> resultSet = new ArrayList<ModelRun>();
        AnalyticTest analyticTest = analyticTestEntityMgr.findByName(analyticTestName);
        if (analyticTest == null) {
            throw new LedpException(LedpCode.LEDP_35000, new String[] { "Analytic Test", analyticTestName });
        }
        if (analyticTest.isExecuted()) {
            return modelRunEntityMgr.findModelRunsByAnalyticTest(analyticTestName);
        }

        Environment env = new Environment();
        env.apiHostPort = this.apiHostPort;
        env.tenant = "ModelQuality_Test.ModelQuality_Test.Production";
        env.username = "bnguyen@lattice-engines.com";
        env.password = "tahoe";

        for (AnalyticPipeline ap : analyticTest.getAnalyticPipelines()) {
            for (DataSet ds : analyticTest.getDataSets()) {
                ModelRunEntityNames modelRunEntityNames = new ModelRunEntityNames();
                modelRunEntityNames.setAnalyticPipelineName(ap.getName());
                modelRunEntityNames.setDataSetName(ds.getName());
                modelRunEntityNames
                        .setName(analyticTestName + "_" + ap.getPid() + "_" + ds.getPid() + "_" + UUID.randomUUID());
                modelRunEntityNames.setDescription("ModelRun created by the Execute Analytic Test API");
                modelRunEntityNames.setAnalyticTestName(analyticTestName);
                modelRunEntityNames.setAnalyticTestTag(analyticTest.getAnalyticTestTag());
                ModelRun modelRun = modelRunService.createModelRun(modelRunEntityNames, env);
                resultSet.add(modelRun);
            }
        }
        analyticTest.setExecuted(true);
        analyticTestEntityMgr.update(analyticTest);
        return resultSet;
    }

    @Transactional
    public AnalyticTestEntityNames getByName(String analyticTestName) {
        AnalyticTest atest = analyticTestEntityMgr.findByName(analyticTestName);
        if (atest == null) {
            throw new LedpException(LedpCode.LEDP_35000, new String[] { "Analytic Test", analyticTestName });
        }

        AnalyticTestEntityNames result = new AnalyticTestEntityNames();
        result.setName(atest.getName());
        result.setPropDataMatchType(atest.getPropDataMatchType());
        result.setAnalyticTestType(atest.getAnalyticTestType());
        result.setAnalyticTestTag(atest.getAnalyticTestTag());

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
            atestName.setAnalyticTestTag(atest.getAnalyticTestTag());
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
