package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.EMRScalingTransformer.TRANSFORMER_NAME;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.EMRScalingConfig;
import com.latticeengines.hadoop.service.EMRCacheService;


@Component(TRANSFORMER_NAME)
public class EMRScalingTransformer extends AbstractTransformer<EMRScalingConfig> {

    private static final Logger log = LoggerFactory.getLogger(EMRScalingTransformer.class);

    public static final String TRANSFORMER_NAME = "emrScalingTransformer";

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private EMRService emrService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected boolean validateConfig(EMRScalingConfig config, List<String> sourceNames) {
        boolean valid = true;
        if (config.getOperation() == null) {
            log.warn("Must specify operation.");
            valid = false;
        }
        if (config.getDelta() == null || config.getDelta() <= 0) {
            log.warn("Invalid value of delta: " + String.valueOf(config.getDelta()));
            valid = false;
        }
        return valid;
    }

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir,
                                                 TransformStep step) {
        if (Boolean.TRUE.equals(useEmr)) {
            EMRScalingConfig config = getConfiguration(step.getConfig());
            EMRScalingConfig.Operation op = config.getOperation();
            String clusterId = emrCacheService.getClusterId();
            InstanceGroup taskGrp = emrService.getTaskGroup(clusterId);
            if (taskGrp == null || taskGrp.getRequestedInstanceCount() == 0) {
                log.error("There is no task group ready for scaling.");
                return false;
            } else {
                int target;
                switch (op) {
                    case ScaleOut:
                        target = scaleOut(config, taskGrp);
                        break;
                    case ScaleIn:
                        target = scaleIn(config, taskGrp);
                        break;
                    default:
                        log.error("Unknown operation: " + op);
                        return false;
                }
                log.info("Scaling task group from " + taskGrp.getRequestedInstanceCount() + " to " + target);
                emrService.scaleTaskGroup(taskGrp, target);
            }
        } else {
            log.info("This stack is not using emr, skip scaling operation.");
        }
        return true;
    }


    private int scaleOut(EMRScalingConfig config, InstanceGroup taskGrp) {
        // only support delta mode for now
        int delta = config.getDelta();
        int requested = taskGrp.getRequestedInstanceCount();
        return Math.max(1, requested + delta);
    }

    private int scaleIn(EMRScalingConfig config, InstanceGroup taskGrp) {
        // only support delta mode for now
        int delta = config.getDelta();
        int requested = taskGrp.getRequestedInstanceCount();
        return Math.max(1, requested - delta);
    }

}
