package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.service.impl.ModelingServiceImpl;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class ModelStepYarnProcessorImplUnitTestNG {

    private ModelStepYarnProcessorImpl processor = new ModelStepYarnProcessorImpl();

    @BeforeClass(groups = "unit")
    public void beforeClass() throws Exception {
        initMocks(this);

        ModelingService modelingService = mock(ModelingServiceImpl.class);
        ReflectionTestUtils.setField(processor, "modelingService", modelingService);
    }

    public ModelCommandParameters createModelCommandParameters(ModelCommand command) {
        return new ModelCommandParameters(command.getCommandParameters());
    }

    @Test(groups = "unit")
    public void testGenerateSamplingConfiguration() {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L);
        SamplingConfiguration samplingConfig = processor.generateSamplingConfiguration(
                ModelStepYarnProcessorImpl.DataSetType.STANDARD, "Nutanix", command,
                createModelCommandParameters(command));
        assertEquals(samplingConfig.getTable(), "Q_EventTable_Nutanix");
        assertEquals(1, samplingConfig.getSamplingElements().size());
    }

    @Test(groups = "unit")
    public void testGenerateModel() {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L);
        Model model = processor.generateModel(ModelStepYarnProcessorImpl.DataSetType.STANDARD, "Nutanix", command,
                createModelCommandParameters(command));
        assertEquals(1, model.getModelDefinition().getAlgorithms().size());
    }

    @Test(groups = "unit")
    public void testCalculateSamplePercentages() {
        int numSamples = 0;
        List<Integer> samples = processor.calculateSamplePercentages(numSamples);
        assertEquals(numSamples, samples.size());

        numSamples = 1;
        samples = processor.calculateSamplePercentages(numSamples);
        assertEquals(numSamples, samples.size());
        assertEquals(100, samples.get(0).intValue());

        numSamples = 2;
        samples = processor.calculateSamplePercentages(numSamples);
        assertEquals(numSamples, samples.size());
        assertEquals(50, samples.get(0).intValue());
        assertEquals(100, samples.get(1).intValue());

        numSamples = 3;
        samples = processor.calculateSamplePercentages(numSamples);
        assertEquals(numSamples, samples.size());
        assertEquals(33, samples.get(0).intValue());
        assertEquals(66, samples.get(1).intValue());
        assertEquals(100, samples.get(2).intValue());

        numSamples = 6;
        samples = processor.calculateSamplePercentages(numSamples);
        assertEquals(numSamples, samples.size());
        assertEquals(16, samples.get(0).intValue());
        assertEquals(32, samples.get(1).intValue());
        assertEquals(48, samples.get(2).intValue());
        assertEquals(64, samples.get(3).intValue());
        assertEquals(80, samples.get(4).intValue());
        assertEquals(100, samples.get(5).intValue());
    }

    @Test(groups = "unit")
    public void testCalculatePriority() {
        assertEquals(0, processor.calculatePriority(0));
        assertEquals(1, processor.calculatePriority(1));
        assertEquals(2, processor.calculatePriority(2));
        assertEquals(2, processor.calculatePriority(3));
    }

    @Test(groups = "unit")
    public void testParsingOfFeatureThreshold() {
        int featureThreshold = 30;

        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L, featureThreshold);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        assertEquals(processor.generateFeatureThreshold(commandParameters), featureThreshold);
    }

    @Test(groups = "unit")
    public void testParsingOfSamplingPercentage() {
        for(int numberOfSamples = 1; numberOfSamples < 5; numberOfSamples++) {
            assertEquals(processor.getSamplePercentage(numberOfSamples), 100 / numberOfSamples );
        }
    }

    @Test(groups = "unit")
    public void testSamplingPercentage() {
        int numSamples = 1;
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParametersNumSamples(1L, numSamples );
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());
        assertEquals(commandParameters.getNumSamples(), numSamples);

        numSamples = 3;
        command = ModelingServiceTestUtils.createModelCommandWithCommandParametersNumSamples(1L, numSamples );
        commandParameters = new ModelCommandParameters(command.getCommandParameters());
        assertEquals(commandParameters.getNumSamples(), numSamples);
    }

}
