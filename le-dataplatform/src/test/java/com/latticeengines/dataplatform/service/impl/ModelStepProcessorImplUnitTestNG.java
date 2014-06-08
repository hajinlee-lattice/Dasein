package com.latticeengines.dataplatform.service.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceImpl;
import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelCommandParameters;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
public class ModelStepProcessorImplUnitTestNG {
    
    private ModelStepProcessorImpl processor = new ModelStepProcessorImpl();
    private static final int MODEL_COMMAND_ID = 1;
    
    @BeforeClass(groups = "unit")
    public void beforeClass() throws Exception {
        initMocks(this);

        ModelingService modelingService = mock(ModelingServiceImpl.class);
        ReflectionTestUtils.setField(processor, "modelingService", modelingService);
    }
    
    public ModelCommandParameters createModelCommandParameters() {
        List<ModelCommandParameter> parameters = new ArrayList<>();
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.DEPIVOTED_EVENT_TABLE, "Q_EventTableDepivot"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.EVENT_TABLE, "Q_EventTable"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.KEY_COLS, "Nutanix_EventTable_Clean"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.METADATA_TABLE, "EventMetadata"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.MODEL_NAME, "Model Submission1"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.MODEL_TARGETS, "P1_Event_1"));
        parameters.add(new ModelCommandParameter(MODEL_COMMAND_ID, ModelCommandParameters.NUM_SAMPLES, "3"));
        
        return processor.validateCommandParameters(parameters);
    }
    
    @Test(groups = "unit", expectedExceptions = LedpException.class)
    public void testInvalidCommandParameters() throws Exception {
        List<ModelCommandParameter> parameters = new ArrayList<>();
        try {
            processor.validateCommandParameters(parameters);
        } catch(LedpException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains(ModelCommandParameters.DEPIVOTED_EVENT_TABLE));
            assertTrue(msg.contains(ModelCommandParameters.EVENT_TABLE));
            assertTrue(msg.contains(ModelCommandParameters.KEY_COLS));
            assertTrue(msg.contains(ModelCommandParameters.METADATA_TABLE));
            assertTrue(msg.contains(ModelCommandParameters.MODEL_NAME));
            assertTrue(msg.contains(ModelCommandParameters.MODEL_TARGETS));
            
            throw e;
        }
    }       
    
    @Test(groups = "unit")
    public void testSplit() {
        List<String> splits = processor.splitCommaSeparatedStringToList("one, two,  three, four");
        assertEquals(4, splits.size());
    }
    
    @Test(groups = "unit")
    public void testGenerateSamplingConfiguration() {
        SamplingConfiguration samplingConfig = processor.generateSamplingConfiguration(ModelStepProcessorImpl.RF_SAMPLENAME_PREFIX, "Nutanix", createModelCommandParameters());
        assertEquals("Q_EventTable", samplingConfig.getTable());
        assertEquals(3, samplingConfig.getSamplingElements().size());
    }
    
    @Test(groups = "unit")
    public void testGenerateModel() {
        Model model = processor.generateModel(ModelStepProcessorImpl.RF_SAMPLENAME_PREFIX, "Nutanix", createModelCommandParameters());
        assertEquals(3, model.getModelDefinition().getAlgorithms().size());        
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
    
}
