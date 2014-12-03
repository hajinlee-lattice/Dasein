package com.latticeengines.skald;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.latticeengines.camille.interfaces.data.DataInterfaceSubscriber;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.skald.model.DataComposition;
import com.latticeengines.domain.exposed.skald.model.ModelCombination;
import com.latticeengines.domain.exposed.skald.model.ModelIdentifier;
import com.latticeengines.domain.exposed.skald.model.ModelTags;
import com.latticeengines.domain.exposed.skald.model.ScoreDerivation;
import com.latticeengines.domain.exposed.skald.model.TargetedModel;

// Retrieves and caches active model structures, transform definitions, and score derivations.
@Service
public class CombinationRetriever {

    public List<CombinationElement> getCombination(CustomerSpace space, String combinationName, String tag) {
        try {
            // TODO Add a caching layer.

            if (combinationName.startsWith("/")) {
                throw new RuntimeException("Invalid combination name: " + combinationName);
            }

            // Retrieve the named ModelCombination structure from ZooKeeper.
            DataInterfaceSubscriber registrationInterface = new DataInterfaceSubscriber(
                    DocumentConstants.REGISTRATION_INTERFACE, space);
            Document combinationDoc = registrationInterface.get(new Path(String.format(DocumentConstants.COMBINATION,
                    combinationName)));
            ModelCombination combination = JsonUtils.deserialize(combinationDoc.getData(), ModelCombination.class);

            Document modelTagsDoc = registrationInterface.get(new Path(DocumentConstants.MODEL_TAGS));
            ModelTags modelTags = JsonUtils.deserialize(modelTagsDoc.getData(), ModelTags.class);

            DataInterfaceSubscriber modelInterface = new DataInterfaceSubscriber(DocumentConstants.MODEL_INTERFACE,
                    space);

            // For each entry in the combination, retrieve the DataComposition
            // and (default or override) ScoreDerivation from ZooKeeper.
            List<CombinationElement> toReturn = new ArrayList<CombinationElement>();
            for (TargetedModel targetedModel : combination) {
                // Bind to actual model version by looking in ModelTags
                Map<String, Integer> tagmap = modelTags.get(targetedModel.model);
                if (tagmap == null) {
                    // Documents are inconsistent. Since writing should be
                    // transactional this should not happen.
                    throw new RuntimeException("There are no tags for model " + targetedModel.model);
                }
                Integer version = tagmap.get(tag);
                if (version == null) {
                    throw new RuntimeException("Model " + targetedModel.model + " does not have a tag " + tag);
                }

                ModelIdentifier identifier = new ModelIdentifier(targetedModel.model, version.intValue());

                // Look up DataComposition
                Document dataCompositionDoc = modelInterface.get(new Path(String.format(
                        DocumentConstants.DATA_COMPOSITION, identifier.name, identifier.version)));
                if (dataCompositionDoc == null) {
                    throw new RuntimeException("No model exists with name " + identifier.name + " and version "
                            + identifier.version);
                }
                DataComposition composition = JsonUtils
                        .deserialize(dataCompositionDoc.getData(), DataComposition.class);

                // Look up ScoreDerivation
                Document scoreDerivationDoc = modelInterface.get(new Path(String.format(
                        DocumentConstants.SCORE_DERIVATION, identifier.name, identifier.version)));
                if (scoreDerivationDoc == null) {
                    throw new RuntimeException("No model exists with name " + identifier.name + " and version "
                            + identifier.version);
                }
                ScoreDerivation scoreDerivation = JsonUtils.deserialize(scoreDerivationDoc.getData(),
                        ScoreDerivation.class);

                // Look up ScoreDerivationOverride
                Document scoreDerivationOverrideDoc = registrationInterface.get(new Path(String.format(
                        DocumentConstants.SCORE_DERIVATION, identifier.name, identifier.version)));
                if (scoreDerivationOverrideDoc != null) {
                    scoreDerivation = JsonUtils
                            .deserialize(scoreDerivationOverrideDoc.getData(), ScoreDerivation.class);
                }

                CombinationElement element = new CombinationElement(targetedModel.filter, composition, identifier,
                        scoreDerivation);
                toReturn.add(element);
            }

            return toReturn;
        } catch (Exception e) {
            throw new RuntimeException("Failure encountered retrieving the combination with name " + combinationName, e);
        }

    }
}
