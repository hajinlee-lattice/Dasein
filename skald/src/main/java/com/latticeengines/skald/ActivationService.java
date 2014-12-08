package com.latticeengines.skald;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.camille.config.ConfigurationController;
import com.latticeengines.camille.util.DocumentUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;
import com.latticeengines.skald.exposed.SetModelCombinationRequest;
import com.latticeengines.skald.exposed.SetModelTagsRequest;
import com.latticeengines.skald.exposed.SetScoreDerivationRequest;

@RestController
public class ActivationService {
    // There are no corresponding get methods because these structures will be
    // exposed by Skald in ZooKeeper through a Camille data interface.

    @RequestMapping(value = "SetModelTags", method = RequestMethod.POST)
    public void setModelTags(@RequestBody SetModelTagsRequest request) {
        log.info(String.format("Received a set model tags request for %s", request.space));

        Path path = new Path(DocumentConstants.MODEL_TAGS);
        setDocument(request.space, path, request.tags);
    }

    @RequestMapping(value = "SetModelCombination", method = RequestMethod.POST)
    public void setModelCombination(@RequestBody SetModelCombinationRequest request) {
        log.info(String.format("Received a set model combination request for %s combination %s", request.space,
                request.name));

        Path path = new Path(String.format(DocumentConstants.COMBINATION, request.name));
        setDocument(request.space, path, request.combination);
    }

    @RequestMapping(value = "SetScoreDerivation", method = RequestMethod.POST)
    public void setScoreDerivation(@RequestBody SetScoreDerivationRequest request) {
        log.info(String.format("Received a set score derivation request for %s model %s version %d", request.space,
                request.model.name, request.model.version));

        Path path = new Path(String.format(DocumentConstants.SCORE_DERIVATION_OVERRIDE, request.model.name,
                request.model.version));
        setDocument(request.space, path, request.derivation);
    }

    private static <T> void setDocument(CustomerSpace space, Path path, T value) {
        try {
            CustomerSpaceServiceScope scope = new CustomerSpaceServiceScope(space, DocumentConstants.SERVICE_NAME,
                    DocumentConstants.DATA_VERSION);
            ConfigurationController<CustomerSpaceServiceScope> controller = ConfigurationController.construct(scope);

            controller.upsert(path, DocumentUtils.toDocument(value));

        } catch (Exception ex) {
            throw new RuntimeException("Failed to set configuration document", ex);
        }

    }

    private static final Log log = LogFactory.getLog(ActivationService.class);
}
