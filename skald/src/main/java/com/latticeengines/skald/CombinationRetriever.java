package com.latticeengines.skald;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.springframework.stereotype.Service;

import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.camille.exposed.interfaces.data.DataInterfaceSubscriber;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.ModelCombination;
import com.latticeengines.domain.exposed.scoringapi.ModelIdentifier;
import com.latticeengines.domain.exposed.scoringapi.ModelTags;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.TargetedModel;

// Retrieves and caches active model structures, transform definitions, and score derivations.
@Service
public class CombinationRetriever {

    public List<CombinationElement> getCombination(CustomerSpace space, String combinationName, String tag) {
        try {
            // TODO Add a caching layer.

            // TODO Add more aggressive combination name validation.
            if (combinationName.startsWith("/")) {
                throw new RuntimeException("Invalid combination name: " + combinationName);
            }

            // Retrieve the ModelTags and the specified ModelCombination.
            CustomerSpaceServiceScope scope = new CustomerSpaceServiceScope(space, DocumentConstants.SERVICE_NAME);
            ConfigurationController<CustomerSpaceServiceScope> controller = ConfigurationController.construct(scope);

            Path tagsPath = new Path(DocumentConstants.MODEL_TAGS);
            ModelTags tags = JsonUtils.deserialize(controller.get(tagsPath).getData(), ModelTags.class);

            Path combinationPath = new Path(String.format(DocumentConstants.COMBINATION, combinationName));
            ModelCombination combination = JsonUtils.deserialize(controller.get(combinationPath).getData(),
                    ModelCombination.class);

            // For each entry in the combination, retrieve the DataComposition
            // and (default or override) ScoreDerivation from ZooKeeper.
            DataInterfaceSubscriber modelInterface = new DataInterfaceSubscriber(DocumentConstants.MODEL_INTERFACE,
                    space);

            List<CombinationElement> toReturn = new ArrayList<CombinationElement>();
            for (TargetedModel targetedModel : combination) {
                // Bind to actual model version by looking in ModelTags
                Map<String, Integer> tagmap = tags.get(targetedModel.model);
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
                Path overridePath = new Path(String.format(DocumentConstants.SCORE_DERIVATION_OVERRIDE,
                        identifier.name, identifier.version));
                try {
                    Document override = controller.get(overridePath);
                    if (override != null) {
                        scoreDerivation = JsonUtils.deserialize(override.getData(), ScoreDerivation.class);
                    }
                } catch (KeeperException.NoNodeException ex) {
                    // Pass
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
