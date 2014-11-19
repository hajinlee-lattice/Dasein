package com.latticeengines.skald;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.skald.model.FieldInterpretation;
import com.latticeengines.skald.model.FieldSchema;
import com.latticeengines.skald.model.FieldSource;
import com.latticeengines.skald.model.FieldType;
import com.latticeengines.skald.model.PredictiveModel;
import com.latticeengines.skald.model.TransformDefinition;

@Service
public class ModelRetriever {
    public List<ModelElement> getModelCombination(CustomerSpace spaceID, String combination) {
        // TODO Add a caching layer.

        PredictiveModel model = new PredictiveModel();

        // TODO Replace with actual retrieval logic.
        try {
            byte[] buffer = Files.readAllBytes(Paths.get("c:\\users\\wbaumann\\desktop\\rfpmml.xml"));
            model.pmml = new String(buffer, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }

        model.transforms = new ArrayList<TransformDefinition>();
        TransformDefinition transform = new TransformDefinition("test_transform", "victory_field", FieldType.Float,
                null);
        model.transforms.add(transform);

        // TODO Retrieve the other structures.
        ModelElement element = new ModelElement();
        element.model = model;
        element.model.fields = new ArrayList<FieldSchema>();
        element.model.fields.add(new FieldSchema("extra", FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.model.fields.add(new FieldSchema("special", FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.model.fields.add(new FieldSchema("magic", FieldSource.Request, FieldType.Integer,
                FieldInterpretation.Feature));

        List<ModelElement> result = new ArrayList<ModelElement>();
        result.add(element);
        return result;
    }
}
