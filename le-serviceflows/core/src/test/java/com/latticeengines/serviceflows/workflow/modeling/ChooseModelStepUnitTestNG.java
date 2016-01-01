package com.latticeengines.serviceflows.workflow.modeling;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ExecutionContext;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

public class ChooseModelStepUnitTestNG {

    private static final CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace.parse("DemoContract.DemoTenant.Production");

    @Test(groups = "unit")
    public void testChooseBestModelIdAndEventColumn() {
        ChooseModel chooseModel = new ChooseModel();

        ExecutionContext executionContext = mock(ExecutionContext.class);
        chooseModel.setExecutionContext(executionContext);
        List<ModelSummary> models = setupModels();
        Map<String, String> modelApplicationIdToEventColumn = new HashMap<>();
        for (ModelSummary model : models) {
            modelApplicationIdToEventColumn.put(model.getApplicationId(), "Event-" + model.getApplicationId());
        }

        Entry<String, String> chosenModelIdAndEventColumn = chooseModel.chooseBestModelIdAndEventColumn(models,
                modelApplicationIdToEventColumn);

        assertEquals(chosenModelIdAndEventColumn.getKey(), "modelValidFirst");
        assertEquals(chosenModelIdAndEventColumn.getValue(), "Event-modelValidFirst");
    }

    @Test(groups = "unit")
    public void testNoDefaultTargetMarketExistsNoValidModelsFallbacktoHighestLift() {
        ChooseModel chooseModel = new ChooseModel();
        ExecutionContext executionContext = mock(ExecutionContext.class);
        chooseModel.setExecutionContext(executionContext);
        ChooseModelStepConfiguration configuration = new ChooseModelStepConfiguration();
        chooseModel.setConfiguration(configuration);
        TargetMarket targetMarket = new TargetMarket();
        targetMarket.setIsDefault(true);
        configuration.setTargetMarket(targetMarket);

        List<ModelSummary> models = setupInvalidModels();
        Map<String, String> modelApplicationIdToEventColumn = new HashMap<>();
        for (ModelSummary model : models) {
            modelApplicationIdToEventColumn.put(model.getApplicationId(), "Event-" + model.getApplicationId());
        }

        Entry<String, String> chosenModelIdAndEventColumn = chooseModel.chooseBestModelIdAndEventColumn(models,
                modelApplicationIdToEventColumn);

        assertEquals(chosenModelIdAndEventColumn.getKey(), "modelInvalidHighestLift");
        assertEquals(chosenModelIdAndEventColumn.getValue(), "Event-modelInvalidHighestLift");
    }

    @Test(groups = "unit")
    public void testFallbacktoGlobalModel() {
        TargetMarket defaultTargetMarket = new TargetMarket();
        defaultTargetMarket.setModelId("GlobalModelId");
        defaultTargetMarket.setEventColumnName("GlobalModelEventColumn");

        Entry<String, String> chosenModelIdAndEventColumn = chooseFallbackToDefaultTargetMarket(defaultTargetMarket);

        assertEquals(chosenModelIdAndEventColumn.getKey(), "GlobalModelId");
        assertEquals(chosenModelIdAndEventColumn.getValue(), "GlobalModelEventColumn");
    }

    @Test(groups = "unit")
    public void testDefaultTargetMarketExistsButNoGlobalModel() {
        TargetMarket defaultTargetMarket = new TargetMarket();
        defaultTargetMarket.setModelId(null);
        defaultTargetMarket.setEventColumnName(null);

        Entry<String, String> chosenModelIdAndEventColumn = chooseFallbackToDefaultTargetMarket(defaultTargetMarket);

        assertEquals(chosenModelIdAndEventColumn.getKey(), "modelInvalidHighestLift");
        assertEquals(chosenModelIdAndEventColumn.getValue(), "Event-modelInvalidHighestLift");
    }

    private Entry<String, String> chooseFallbackToDefaultTargetMarket(TargetMarket defaultTargetMarket) {
        ChooseModel chooseModel = new ChooseModel();
        
        ExecutionContext executionContext = mock(ExecutionContext.class);
        chooseModel.setExecutionContext(executionContext);
        InternalResourceRestApiProxy proxy = mock(InternalResourceRestApiProxy.class);
        chooseModel.setProxy(proxy);
        ChooseModelStepConfiguration configuration = new ChooseModelStepConfiguration();
        configuration.setCustomerSpace(DEMO_CUSTOMERSPACE);
        chooseModel.setConfiguration(configuration);
        TargetMarket targetMarket = new TargetMarket();
        targetMarket.setIsDefault(false);
        configuration.setTargetMarket(targetMarket);
        when(proxy.findTargetMarketByName(TargetMarket.DEFAULT_NAME, configuration.getCustomerSpace().toString())).thenReturn(defaultTargetMarket);

        List<ModelSummary> models = setupInvalidModels();
        Map<String, String> modelApplicationIdToEventColumn = new HashMap<>();
        for (ModelSummary model : models) {
            modelApplicationIdToEventColumn.put(model.getApplicationId(), "Event-" + model.getApplicationId());
        }

        Entry<String, String> chosenModelIdAndEventColumn = chooseModel.chooseBestModelIdAndEventColumn(models,
                modelApplicationIdToEventColumn);

        return chosenModelIdAndEventColumn;
    }

    private List<ModelSummary> setupInvalidModels() {
        List<ModelSummary> models = new ArrayList<>();
        ModelSummary modelInvalidLowRoc = new ModelSummary();
        modelInvalidLowRoc.setId("modelInvalidLowRoc");
        modelInvalidLowRoc.setApplicationId("modelInvalidLowRoc");
        modelInvalidLowRoc.setTop20PercentLift(2d);
        modelInvalidLowRoc.setTotalConversionCount(ChooseModel.MINIMUM_POSITIVE_EVENTS);
        modelInvalidLowRoc.setTotalRowCount(10000L);
        modelInvalidLowRoc.setRocScore(ChooseModel.MINIMUM_ROC - 0.1);
        models.add(modelInvalidLowRoc);

        ModelSummary modelInvalidLowConversion = new ModelSummary();
        modelInvalidLowConversion.setId("modelInvalidLowConversion");
        modelInvalidLowConversion.setApplicationId("modelInvalidLowConversion");
        modelInvalidLowConversion.setTop20PercentLift(3d);
        modelInvalidLowConversion.setTotalConversionCount(ChooseModel.MINIMUM_POSITIVE_EVENTS - 1);
        modelInvalidLowConversion.setTotalRowCount(10000L);
        modelInvalidLowConversion.setRocScore(ChooseModel.MINIMUM_ROC);
        models.add(modelInvalidLowConversion);

        ModelSummary modelInvalidHighestLift = new ModelSummary();
        modelInvalidHighestLift.setId("modelInvalidHighestLift");
        modelInvalidHighestLift.setApplicationId("modelInvalidHighestLift");
        modelInvalidHighestLift.setTop20PercentLift(5d);
        modelInvalidHighestLift.setTotalConversionCount(ChooseModel.MINIMUM_POSITIVE_EVENTS - 1);
        modelInvalidHighestLift.setTotalRowCount(10000L);
        modelInvalidHighestLift.setRocScore(ChooseModel.MINIMUM_ROC - 0.1);
        models.add(modelInvalidHighestLift);

        return models;
    }

    private List<ModelSummary> setupModels() {
        List<ModelSummary> models = new ArrayList<>();
        models.addAll(setupInvalidModels());

        ModelSummary modelValidFirst = new ModelSummary();
        modelValidFirst.setId("modelValidFirst");
        modelValidFirst.setApplicationId("modelValidFirst");
        modelValidFirst.setTop20PercentLift(3d);
        modelValidFirst.setTotalConversionCount(ChooseModel.MINIMUM_POSITIVE_EVENTS);
        modelValidFirst.setTotalRowCount(10000L);
        modelValidFirst.setRocScore(ChooseModel.MINIMUM_ROC);
        models.add(modelValidFirst);

        ModelSummary modelValidSecond = new ModelSummary();
        modelValidSecond.setId("modelValidSecond");
        modelValidSecond.setApplicationId("modelValidSecond");
        modelValidSecond.setTop20PercentLift(2d);
        modelValidSecond.setTotalConversionCount(ChooseModel.MINIMUM_POSITIVE_EVENTS);
        modelValidSecond.setTotalRowCount(10000L);
        modelValidSecond.setRocScore(ChooseModel.MINIMUM_ROC);
        models.add(modelValidSecond);

        return models;
    }

}
