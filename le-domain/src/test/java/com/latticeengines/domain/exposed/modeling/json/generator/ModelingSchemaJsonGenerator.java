package com.latticeengines.domain.exposed.modeling.json.generator;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.AlgorithmBase;

public final class ModelingSchemaJsonGenerator {

    protected ModelingSchemaJsonGenerator() {
        throw new UnsupportedOperationException();
    }

    public static void main(String[] args) throws IOException {
        List<Class<?>> classList = Arrays.asList(//
                AlgorithmBase.class, //
                LoadConfiguration.class, //
                SamplingConfiguration.class, //
                SamplingElement.class, //
                DataProfileConfiguration.class, //
                Model.class //
        );
        for (Class<?> cls : classList) {
            ObjectMapper mapper = new ObjectMapper();
            JsonSchema jsonSchema = new JsonSchemaGenerator(mapper).generateSchema(cls);
            FileUtils.writeStringToFile(new File(cls.getSimpleName().toLowerCase() + ".json"), //
                    jsonSchema.toString(), "UTF-8");
            System.out.println(jsonSchema);
        }
    }
}
