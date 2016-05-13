package com.latticeengines.dataplatform.service.impl.validation;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.service.DispatchService;
import com.latticeengines.dataplatform.service.ModelValidationService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

@Component("defaultModelValidationService")
public class DefaultModelValidationServiceImpl extends ModelValidationService {

    private static final String DIAGNOSTIC_FILE = "diagnostics.json";

    @Autowired
    private Configuration yarnConfiguration;
    
    @Resource(name = "parallelDispatchService")
    private DispatchService dispatchService;

    @Value("${dataplatform.modeling.row.threshold:50}")
    private int rowSizeThreshold;

    public DefaultModelValidationServiceImpl() {
        super(new String[] { new DecisionTreeAlgorithm().getName(), //
                              new RandomForestAlgorithm().getName(), //
                              new LogisticRegressionAlgorithm().getName() //
                });
    }

    @Override
    public void validate(Model model) throws Exception {
        String diagnosticsPath = model.getMetadataHdfsPath() + "/" + DIAGNOSTIC_FILE;

        if (!HdfsUtils.fileExists(yarnConfiguration, diagnosticsPath)) {
            throw new LedpException(LedpCode.LEDP_15004);
        }
        // Parse diagnostics file
        long sampleSize = dispatchService.getSampleSize(yarnConfiguration, diagnosticsPath,
                model.isParallelEnabled());

        if (sampleSize < rowSizeThreshold) {
            throw new LedpException(LedpCode.LEDP_15005, new String[] { Double.toString(sampleSize) });
        }
    }

}
