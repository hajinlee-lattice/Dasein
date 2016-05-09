package com.latticeengines.datascience.utils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsonschema.JsonSchema;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;


public class ModelingSchemaJsonGenerator {

    public static void main(String[] args) throws IOException {
        List<Class<?>> classList = Arrays.<Class<?>> asList(getClassesToGenerate());
        for (Class<?> cls : classList) {
            ObjectMapper mapper = new ObjectMapper();
            JsonSchema jsonSchema = mapper.generateJsonSchema(cls);
            FileUtils.writeStringToFile(new File(cls.getSimpleName().toLowerCase() + ".json"), jsonSchema.toString());
            System.out.println(jsonSchema);
        }
    }
    
    static Class<?>[] getClassesToGenerate() {
        return new Class[] { AlgorithmBase.class, //
                LoadConfiguration.class, //
                SamplingConfiguration.class, //
                SamplingElement.class, //
                DataProfileConfiguration.class, //
                Model.class };
    }
}