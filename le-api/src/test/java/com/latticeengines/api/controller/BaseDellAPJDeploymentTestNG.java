package com.latticeengines.api.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

public class BaseDellAPJDeploymentTestNG extends ApiFunctionalTestNGBase {

    Model getModel(String customer) {
        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(0);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=2048 PRIORITY=2");
        randomForestAlgorithm.setSampleName("s0");
        randomForestAlgorithm.setPipelineScript("/datascience/playmaker/evmodel/evpipeline.py");
        randomForestAlgorithm.setPipelineLibScript("/datascience/playmaker/evmodel/evpipeline.tar.gz");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Random Forest against all");
        modelDef.addAlgorithms(Collections.singletonList(randomForestAlgorithm));

        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("INTERNAL_DellAPJDeploymentTestNG Random Forest Model on raw Data");
        model.setTable("Play_11_TrainingSample_WithRevenue");
        model.setMetadataTable("Play_11_EventMetadata");
        model.setCustomer(customer);
        model.setKeyCols(Collections.singletonList("AnalyticPurchaseState_ID"));
        model.setProvenanceProperties("EVModelColumns=__Revenue_0,__Revenue_1,__Revenue_2,__Revenue_3,__Revenue_4,__Revenue_5,__Revenue_6,__Revenue_7,__Revenue_8,__Revenue_9,__Revenue_10,__Revenue_11 DataLoader_Query=x DataLoader_TenantName=y DataLoader_Instance=z");
        model.setDataFormat("avro");

        return model;
    }

    @Deprecated
    LoadConfiguration getLoadConfig(Model model) {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dataSourceHost).port(dataSourcePort).db(dataSourceDB).user(dataSourceUser)
                .clearTextPassword(dataSourcePasswd).dbType(dataSourceDBType);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer(model.getCustomer());
        config.setTable("Play_11_TrainingSample_WithRevenue");
        config.setKeyCols(Collections.singletonList("AnalyticPurchaseState_ID"));
        return config;
    }

    SamplingConfiguration getSampleConfig(Model model) {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.setCustomer(model.getCustomer());
        samplingConfig.setTable(model.getTable());
        return samplingConfig;
    }

    DataProfileConfiguration getProfileConfig(Model model) {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(model.getCustomer());
        config.setTable(model.getTable());
        config.setMetadataTable(model.getMetadataTable());
        config.setSamplePrefix("s0");
        config.setTargets(Collections.singletonList("Target"));
        config.setExcludeColumnList(getExcludeList());
        return config;
    }

    private List<String> getExcludeList() {
        return new ArrayList<>(Arrays.asList("Ext_LEAccount_PD_FundingDateUpdated", //
                "Ext_LEAccount_PD_FundingDate", //
                "AnalyticPurchaseState_ID", //
                "Offset", //
                "LEAccount_ID", //
                "Period_ID", //
                "Target", //
                "Product_2_RevenueMomentum3", //
                "roduct_36_Revenue", //
                "Product_36_RevenueMomentum3", //
                "Product_62_RevenueMomentum3", //
                "Product_36_Units", //
                "Product_62_Revenue", //
                "Product_62_Units", //
                "Train"));
    }

}
