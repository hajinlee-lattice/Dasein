package com.latticeengines.api.controller;

import java.util.Collections;

import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;

public class BaseModelResourceDeploymentTestNG extends ApiFunctionalTestNGBase {

    @Deprecated
    LoadConfiguration getLoadConfig(Model model) {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dataSourceHost) //
                .port(dataSourcePort) //
                .db(dataSourceDB) //
                .user(dataSourceUser) //
                .clearTextPassword(dataSourcePasswd) //
                .dbType(dataSourceDBType);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer(model.getCustomer());
        config.setTable("iris");
        config.setKeyCols(Collections.singletonList("ID"));
        config.setMetadataTable("EventMetadata");
        return config;
    }

    SamplingConfiguration getSampleConfig(Model model) {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(60);
        SamplingElement all = new SamplingElement();
        all.setName("all");
        all.setPercentage(100);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.addSamplingElement(s1);
        samplingConfig.addSamplingElement(all);
        samplingConfig.setCustomer(model.getCustomer());
        samplingConfig.setTable(model.getTable());
        return samplingConfig;
    }

    DataProfileConfiguration getProfileConfig(Model model) {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(model.getCustomer());
        config.setTable(model.getTable());
        config.setMetadataTable(model.getMetadataTable());
        config.setIncludeColumnList(model.getFeaturesList());
        config.setSamplePrefix("all");
        config.setTargets(model.getTargetsList());
        return config;
    }

}
