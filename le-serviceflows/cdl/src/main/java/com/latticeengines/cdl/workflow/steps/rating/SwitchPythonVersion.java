package com.latticeengines.cdl.workflow.steps.rating;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("switchPythonVersion")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SwitchPythonVersion extends BaseWorkflowStep<GenerateRatingStepConfiguration> {

    @Override
    public void execute() {
        String p2ResultTable = getStringValueFromContext(AI_RAW_RATING_TABLE_NAME);
        if (StringUtils.isNotBlank(p2ResultTable)) {
            putStringValueInContext(AI_RAW_RATING_TABLE_NAME_P2, p2ResultTable);
            removeObjectFromContext(AI_RAW_RATING_TABLE_NAME);
        }
        putObjectInContext(ITERATION_AI_RATING_MODELS, getPython3Models());
        putStringValueInContext(PYTHON_MAJOR_VERSION, "3");
    }

    private List<RatingModelContainer> getPython3Models() {
        return getListObjectFromContext(ITERATION_RATING_MODELS, RatingModelContainer.class).stream() //
                .filter(container -> {
                    RatingEngineType ratingEngineType = container.getEngineSummary().getType();
                    boolean isAIEngine = RatingEngineType.CROSS_SELL.equals(ratingEngineType)
                            || RatingEngineType.CUSTOM_EVENT.equals(ratingEngineType);
                    if (isAIEngine) {
                        if (container.getModel() instanceof AIModel) {
                            String modelPythonVersion = ((AIModel) container.getModel()).getPythonMajorVersion();
                            return "3".equals(modelPythonVersion);
                        } else {
                            return true;
                        }
                    } else {
                        return false;
                    }
                }) //
                .collect(Collectors.toList());
    }

}
