package com.latticeengines.dataplatform.service.impl.validation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.ModelValidationService;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.algorithm.PMMLAlgorithm;

@Component("pmmlModelValidationService")
public class PMMLModelValidationServiceImpl extends ModelValidationService {

    public PMMLModelValidationServiceImpl() {
        super(new String[] { new PMMLAlgorithm().getName() });
    }

    @Override
    public void validate(Model model) throws Exception {
    }

}
