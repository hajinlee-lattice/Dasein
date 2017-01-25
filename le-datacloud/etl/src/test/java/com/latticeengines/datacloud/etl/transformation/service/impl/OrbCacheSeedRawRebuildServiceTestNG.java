package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.OrbCacheSeed;
import com.latticeengines.datacloud.core.source.impl.OrbCompanyRaw;
import com.latticeengines.datacloud.core.source.impl.OrbDomainRaw;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.OrbCacheSeedRebuildConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig;

public class OrbCacheSeedRawRebuildServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    @Autowired
    OrbCacheSeed source;

    @Autowired
    OrbCompanyRaw baseSourceOrbCompanyRaw;

    @Autowired
    OrbDomainRaw baseSourceOrbDomainRaw;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "OrbCacheSeed";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSourceOrbCompanyRaw, "OrbCompanyRaw", baseSourceVersion);
        uploadBaseSourceFile(baseSourceOrbDomainRaw, "OrbDomainRaw", baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    String getPathToUploadBaseData() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        return hdfsPathBuilder.constructSnapshotDir(targetSource, targetVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            configuration.setName("OrbCacheSeedRawRebuild");
            configuration.setVersion(targetVersion);

            // Field standardization for OrbCompany
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add("OrbCompanyRaw");
            step1.setBaseSources(baseSources);
            step1.setTransformer("standardizationTransformer");
            step1.setTargetSource("OrbCompanyRawMarked");
            String confParamStr1 = getStandardizationTransformerConfigForOrbCompanyMarker();
            step1.setConfiguration(confParamStr1);

            // Data cleanup for OrbCompany
            TransformationStepConfig step2 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<Integer>();
            inputSteps.add(0);
            step2.setInputSteps(inputSteps);
            step2.setTransformer("standardizationTransformer");
            step2.setTargetSource("OrbCompany");
            String confParamStr2 = getStandardizationTransformerConfigForOrbCompanyCleanup();
            step2.setConfiguration(confParamStr2);

            // Field standardization for OrbDomain
            TransformationStepConfig step3 = new TransformationStepConfig();
            baseSources = new ArrayList<String>();
            baseSources.add("OrbDomainRaw");
            step3.setBaseSources(baseSources);
            step3.setTransformer("standardizationTransformer");
            step3.setTargetSource("OrbDomain");
            String confParamStr3 = getStandardizationTransformerConfigForOrbDomain();
            step3.setConfiguration(confParamStr3);

            // Generate OrbCacheSeed
            TransformationStepConfig step4 = new TransformationStepConfig();
            baseSources = new ArrayList<String>();
            baseSources.add("OrbCompany");
            baseSources.add("OrbDomain");
            step4.setBaseSources(baseSources);
            step4.setTransformer("orbCacheSeedRebuildTransformer");
            step4.setTargetSource(targetSourceName);
            String confParamStr4 = getOrbCacheSeedRebuildConfig();
            step4.setConfiguration(confParamStr4);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);
            steps.add(step3);
            steps.add(step4);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getStandardizationTransformerConfigForOrbCompanyMarker() throws JsonProcessingException {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] domainFields = { "Website" };
        conf.setDomainFields(domainFields);
        String[] countryFields = { "Country" };
        conf.setCountryFields(countryFields);
        String[] stringToIntFields = { "Employee", "LocationEmployee" };
        conf.setStringToIntFields(stringToIntFields);
        String[] stringToLongFields = { "FacebookLikes", "TwitterFollowers", "TotalAmountRaised",
                "LastFundingRoundAmount", "SearchRank" };
        conf.setStringToLongFields(stringToLongFields);
        String[] dedupFields = { "OrbNum" };
        conf.setDedupFields(dedupFields);
        String markerExpression = "OrbNum != null && Name != null && Country != null && Website != null";
        conf.setMarkerExpression(markerExpression);
        String[] markerCheckFields = { "OrbNum", "Name", "Country", "Website" };
        conf.setMarkerCheckFields(markerCheckFields);
        String markerField = "IsValid";
        conf.setMarkerField(markerField);
        return om.writeValueAsString(conf);
    }

    private String getStandardizationTransformerConfigForOrbCompanyCleanup() throws JsonProcessingException {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String filterExpression = "IsValid == true";
        conf.setFilterExpression(filterExpression);
        String[] filterFields = { "IsValid" };
        conf.setFilterFields(filterFields);
        return om.writeValueAsString(conf);
    }

    private String getStandardizationTransformerConfigForOrbDomain() throws JsonProcessingException {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] domainFields = { "WebDomain" };
        conf.setDomainFields(domainFields);
        return om.writeValueAsString(conf);
    }

    private String getOrbCacheSeedRebuildConfig() throws JsonProcessingException {
        OrbCacheSeedRebuildConfig conf = new OrbCacheSeedRebuildConfig();
        conf.setCompanyFileOrbNumField("OrbNum");
        conf.setCompanyFileEntityTypeField("EntityType");
        conf.setCompanyFileDomainField("Website");
        conf.setCompanyFileWebDomainsField("WebDomain");
        conf.setDomainFileOrbNumField("OrbNum");
        conf.setDomainFileDomainField("WebDomain");
        conf.setDomainFileHasEmailField("DomainHasEmail");
        conf.setOrbCacheSeedDomainField("Domain");
        conf.setOrbCacheSeedPrimaryDomainField("PrimaryDomain");
        conf.setOrbCacheSeedIsSecondaryDomainField("IsSecondaryDomain");
        conf.setOrbCacheSeedDomainHasEmailField("DomainHasEmail");
        return om.writeValueAsString(conf);
    }

    @Override
    String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSource, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // TODO Auto-generated method stub

    }
}

