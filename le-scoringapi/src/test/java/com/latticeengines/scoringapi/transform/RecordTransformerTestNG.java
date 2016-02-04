package com.latticeengines.scoringapi.transform;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.modeling.ModelExtractor;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.scoringapi.functionalframework.ScoringApiFunctionalTestNGBase;

public class RecordTransformerTestNG extends ScoringApiFunctionalTestNGBase {
    
    private File modelExtractionDir;
    
    @Autowired
    private RecordTransformer recordTransformer;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }
    
    @DataProvider(name = "tenants")
    public Object[][] getTenants() {
        URL url = ClassLoader.getSystemResource("com/latticeengines/scoringapi/transform/tenants");
        File transformDir = new File(url.getFile());
        
        List<File> tenantList = Arrays.asList(transformDir.listFiles());
        Object[][] tenants = new Object[tenantList.size()][1];
        
        int i = 0;
        for (File tenant : tenantList) {
            tenants[i++][0] = tenant.getAbsolutePath();
        }
        
        return tenants;
    }
    
    @AfterMethod(groups = "functional")
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(modelExtractionDir);
    }

    @Test(groups = "functional", dataProvider = "tenants")
    public void transform(String tenantPath) throws Exception {
        String modelFilePath = tenantPath + "/model.json";
        String dataToScorePath = tenantPath + "/datatoscore.avro";
        String dataCompositionPath = tenantPath + "/datacomposition.json";
        modelExtractionDir = new File(String.format("/tmp/%s/", new File(tenantPath).getName()));
        modelExtractionDir.mkdir();
        
        new ModelExtractor().extractModelArtifacts(modelFilePath, modelExtractionDir.getAbsolutePath());
        
        DataComposition dataComposition = JsonUtils.deserialize( //
                FileUtils.readFileToString(new File(dataCompositionPath)), DataComposition.class);
        
        transform(dataToScorePath, dataComposition.transforms);
    }
    
    private void transform(String avroFile, List<TransformDefinition> transforms) throws Exception {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", "file:///");
        FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(config, new Path(avroFile));
        Schema schema = AvroUtils.getSchema(config, new Path(avroFile));
        for (GenericRecord record : reader) {
            Map<String, Object> recordAsMap = new HashMap<>();
            for (Field f : schema.getFields()) {
                Object value = record.get(f.name());
                if (value instanceof Utf8) {
                    value = ((Utf8) value).toString();
                }
                recordAsMap.put(f.name(), value);
            }
            recordTransformer.transform(modelExtractionDir.getAbsolutePath(), transforms, recordAsMap);
        }
    }
}
