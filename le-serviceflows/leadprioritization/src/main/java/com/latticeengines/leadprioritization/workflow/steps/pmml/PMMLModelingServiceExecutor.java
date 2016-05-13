package com.latticeengines.leadprioritization.workflow.steps.pmml;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.algorithm.PMMLAlgorithm;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

public class PMMLModelingServiceExecutor extends ModelingServiceExecutor {
    
    private static final Log log = LogFactory.getLog(PMMLModelingServiceExecutor.class);

    public PMMLModelingServiceExecutor(Builder builder) {
        super(builder);
    }

    @Override
    public String model() throws Exception {
        PMMLAlgorithm pmmlAlgorithm = new PMMLAlgorithm();
        pmmlAlgorithm.setPriority(0);
        pmmlAlgorithm.setSampleName("all");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("PMML");
        modelDef.addAlgorithms(Collections.singletonList((Algorithm) pmmlAlgorithm));

        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName(builder.getModelName());
        model.setTable(builder.getTable());
        model.setMetadataTable(builder.getMetadataTable());
        model.setCustomer(builder.getCustomer());
        model.setKeyCols(Arrays.asList(new String[] { builder.getKeyColumn() }));
        model.setDataFormat("avro");
        model.setTargetsList(Arrays.asList(builder.getTargets()));
        model.setFeaturesList(Arrays.asList(builder.getFeatureList()));
        model.setSchemaContents(builder.getSchemaContents());

        AppSubmission submission = modelProxy.submit(model);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for modeling: %s", appId));
        JobStatus status = waitForModelingAppId(appId);
        
        // Wait for 30 seconds before retrieving the result directory
        Thread.sleep(30 * 1000L);
        String resultDir = status.getResultDirectory();

        if (resultDir != null) {
            return appId;
        } else {
            log.warn(String.format("No result directory for modeling job %s", appId));
            System.out.println(String.format("No result directory for modeling job %s", appId));
            throw new LedpException(LedpCode.LEDP_28014, new String[] { appId });
        }
    }
    
    public void writeDataFiles() throws Exception {
        File localFile = createDummyTrainingAndTestData(builder.getSchemaContents());
        String trainingDataHdfsPath = String.format("%s/%s/data/%s/samples/allTraining.avro", //
                modelingServiceHdfsBaseDir, builder.getCustomer(), builder.getTable());
        String testDataHdfsPath = String.format("%s/%s/data/%s/samples/allTest.avro", //
                modelingServiceHdfsBaseDir, builder.getCustomer(), builder.getTable());
        
        try {
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), trainingDataHdfsPath);
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), testDataHdfsPath);
        } finally {
            FileUtils.deleteQuietly(localFile);
        }
    }

    private File createDummyTrainingAndTestData(String schemaContents) throws Exception {
        Schema schema = new Schema.Parser().parse(schemaContents);
        File f = new File("PMMLDummyFile-" + System.currentTimeMillis() + ".avro");
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))) {
            dataFileWriter.create(schema, f);
        }
        return f;
    }
    


}