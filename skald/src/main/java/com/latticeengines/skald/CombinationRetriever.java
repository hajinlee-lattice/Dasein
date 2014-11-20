package com.latticeengines.skald;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.skald.model.FieldInterpretation;
import com.latticeengines.skald.model.FieldSchema;
import com.latticeengines.skald.model.FieldSource;
import com.latticeengines.skald.model.FieldType;
import com.latticeengines.skald.model.DataComposition;
import com.latticeengines.skald.model.TransformDefinition;

// Retrieves and caches active model structures, transform definitions, and score derivations.
@Service
public class CombinationRetriever {
    public List<CombinationElement> getCombination(CustomerSpace spaceID, String combination) {
        // TODO Add a caching layer.

        DataComposition data = new DataComposition();

        data.transforms = new ArrayList<TransformDefinition>();
        TransformDefinition transform = new TransformDefinition("test_transform", "victory_field", FieldType.Float,
                null);
        data.transforms.add(transform);

        // TODO Retrieve the other structures.
        CombinationElement element = new CombinationElement();
        element.data = data;
        element.data.fields = new ArrayList<FieldSchema>();
        element.data.fields.add(new FieldSchema("extra", FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.add(new FieldSchema("special", FieldSource.Request, FieldType.Float,
                FieldInterpretation.Feature));
        element.data.fields.add(new FieldSchema("magic", FieldSource.Request, FieldType.Integer,
                FieldInterpretation.Feature));

        List<CombinationElement> result = new ArrayList<CombinationElement>();
        result.add(element);
        return result;
    }
}
