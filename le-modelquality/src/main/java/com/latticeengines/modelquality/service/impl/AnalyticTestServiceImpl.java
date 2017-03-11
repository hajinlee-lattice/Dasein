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
import com.latticeengines.domain.exposed.modelquality.AnalyticTestType;
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

    @Value("${modelquality.pls.login.tenant}")
    private String tenant;

    @Value("${modelquality.pls.login.username")
    private String username;

    @Value("${modelquality.pls.login.password")
    private String password;

    @Override
    public AnalyticTest createAnalyticTest(AnalyticTestEntityNames analyticTestEntityNames) {

        AnalyticTest analyticTest = new AnalyticTest();
        if (analyticTestEntityNames.getName() == null || analyticTestEntityNames.getName().trim().isEmpty()) {
            throw new LedpException(LedpCode.LEDP_35003, new String[]{"AnalyticTest Name"});
        }

        if (analyticTestEntityMgr.findByName(analyticTestEntityNames.getName()) != null) {
            throw new LedpException(LedpCode.LEDP_35002,
                    new String[]{"AnalyticTest", analyticTestEntityNames.getName()});
        }

        analyticTest.setName(analyticTestEntityNames.getName());
        analyticTest.setAnalyticTestType(analyticTestEntityNames.getAnalyticTestType());
        analyticTest.setAnalyticTestTag(analyticTestEntityNames.getAnalyticTestTag());

        if (analyticTestEntityNames.getAnalyticPipelineNames() == null
                || analyticTestEntityNames.getAnalyticPipelineNames().isEmpty()) {
            throw new LedpException(LedpCode.LEDP_35003, new String[]{"AnalyticPipelines"});
        } else {
            ArrayList<AnalyticPipeline> analyticPipelines = new ArrayList<AnalyticPipeline>();
            for (String analyticPipelineName : analyticTestEntityNames.getAnalyticPipelineNames()) {
                AnalyticPipeline ap = analyticPipelineEntityMgr.findByName(analyticPipelineName);
                if (ap != null) {
                    analyticPipelines.add(ap);
                } else {
                    throw new LedpException(LedpCode.LEDP_35000,
                            new String[]{"Analytic pipeline", analyticPipelineName});
                }
            }
            analyticTest.setAnalyticPipelines(analyticPipelines);
        }

        if (analyticTestEntityNames.getDataSetNames() == null || analyticTestEntityNames.getDataSetNames().isEmpty()) {
            throw new LedpException(LedpCode.LEDP_35003, new String[]{"Datasets"});
        } else {
            ArrayList<DataSet> dataSets = new ArrayList<DataSet>();
            for (String datasetName : analyticTestEntityNames.getDataSetNames()) {
                DataSet ds = dataSetEntityMgr.findByName(datasetName);
                if (ds != null) {
                    dataSets.add(ds);
                } else {
                    throw new LedpException(LedpCode.LEDP_35000, new String[]{"Dataset", datasetName});
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
            throw new LedpException(LedpCode.LEDP_35000, new String[]{"Analytic Test", analyticTestName});
        }
        if (analyticTest.isExecuted()) {
            return modelRunEntityMgr.findModelRunsByAnalyticTest(analyticTestName);
        }

        for (AnalyticPipeline ap : analyticTest.getAnalyticPipelines()) {
            for (DataSet ds : analyticTest.getDataSets()) {
                ModelRun modelRun = createModelRun(analyticTest, ap, ds);
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
            throw new LedpException(LedpCode.LEDP_35000, new String[]{"Analytic Test", analyticTestName});
        }

        AnalyticTestEntityNames result = new AnalyticTestEntityNames();
        result.setName(atest.getName());
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

    public List<AnalyticTest> updateProductionAnalyticPipeline() {
        AnalyticPipeline latestProdPipeline = analyticPipelineEntityMgr.getLatestProductionVersion();
        if (latestProdPipeline == null || latestProdPipeline.getVersion() <= 1) {
            return null;
        }

        AnalyticPipeline previousProdPipeline = analyticPipelineEntityMgr
                .findByVersion(new Integer(latestProdPipeline.getVersion() - 1));

        List<AnalyticTest> aTestsToUpdate = analyticTestEntityMgr.findAllByAnalyticPipeline(previousProdPipeline);

        for (AnalyticTest aTestToUpdate : aTestsToUpdate) {
            updateProductionPipline(aTestToUpdate, latestProdPipeline, previousProdPipeline);
        }

        for (AnalyticTest aTest : aTestsToUpdate) {
            for (DataSet ds : aTest.getDataSets()) {
                createModelRun(aTest, latestProdPipeline, ds);
            }
        }
        return aTestsToUpdate;
    }

    @Transactional
    private void updateProductionPipline(AnalyticTest aTestToUpdate, AnalyticPipeline latestProdPipeline,
                                         AnalyticPipeline previousProdPipeline) {
        if (aTestToUpdate.getAnalyticTestType() == AnalyticTestType.SelectedPipelines) {
            for (int i = 0; i < aTestToUpdate.getAnalyticPipelines().size(); i++) {
                if (aTestToUpdate.getAnalyticPipelines().get(i).getName().equals(previousProdPipeline.getName())) {
                    aTestToUpdate.getAnalyticPipelines().remove(i);
                    break;
                }
            }
        }
        aTestToUpdate.getAnalyticPipelines().add(latestProdPipeline);
        analyticTestEntityMgr.update(aTestToUpdate);
    }

    private ModelRun createModelRun(AnalyticTest analyticTest, AnalyticPipeline ap, DataSet ds) {
        Environment env = new Environment();
        env.apiHostPort = this.apiHostPort;
        env.tenant = this.tenant;
        env.username = this.username;
        env.password = this.password;

        String validatedAnalyticTestName = (analyticTest.getName() != null)
                ? analyticTest.getName().replaceAll("[^A-Za-z0-9_]", "") : "";

        ModelRunEntityNames modelRunEntityNames = new ModelRunEntityNames();
        modelRunEntityNames.setAnalyticPipelineName(ap.getName());
        modelRunEntityNames.setDataSetName(ds.getName());
        modelRunEntityNames
                .setName(validatedAnalyticTestName + "_" + ap.getPid() + "_" + ds.getPid() + "_" + UUID.randomUUID());
        modelRunEntityNames.setDescription("ModelRun created by the Execute Analytic Test API");
        modelRunEntityNames.setAnalyticTestName(analyticTest.getName());
        modelRunEntityNames.setAnalyticTestTag(analyticTest.getAnalyticTestTag());
        ModelRun modelRun = modelRunService.createModelRun(modelRunEntityNames, env);
        return modelRun;
    }
}
