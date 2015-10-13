package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ModelSummaryParserTestNG extends PlsFunctionalTestNGBase {

    @Value("${pls.default.buyerinsights.predictor.number}")
    private int defaultBiPredictorNum;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Test(groups = { "unit", "functional" })
    public void parse() throws Exception {
        InputStream is = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/pls/functionalframework/modelsummary-eloqua.json");
        String data = new String(IOUtils.toByteArray(is));
        ModelSummary summary = modelSummaryParser.parse("modelsummary-eloqua.json", data);

        assertEquals(summary.getName(), "PLSModel-Eloqua-02/18/2015 07:25:38 GMT");
        assertEquals(summary.getLookupId(), "TENANT1|Q_PLS_Modeling_TENANT1|8195dcf1-0898-4ad3-b94d-0d0f806e979e");
        assertEquals(summary.getTrainingRowCount().longValue(), 15376L);
        assertEquals(summary.getTestRowCount().longValue(), 3738L);
        assertEquals(summary.getTotalRowCount().longValue(), 19114L);
        assertEquals(summary.getTrainingConversionCount().longValue(), 719L);
        assertEquals(summary.getTestConversionCount().longValue(), 154L);
        assertEquals(summary.getTotalConversionCount().longValue(), 873L);
        assertEquals(summary.getRocScore(), 0.9341374179555253);
        assertEquals(summary.getId(), "ms__8195dcf1-0898-4ad3-b94d-0d0f806e979e-PLSModel-Eloqua");
        assertFalse(summary.isIncomplete(), "This model summary shouldn't be incomplete.");
        List<Predictor> predictors = summary.getPredictors();
        assertEquals(predictors.size(), 182);
        assertEquals(predictors.get(0).getModelSummary(), summary);
        assertEquals(predictors.get(0).getTenantId().longValue(), -1L);
        assertTrue(topPredictorsAreSortedAndSet(predictors));
        String decompressedDetails = new String(CompressionUtils.decompressByteArray(summary.getDetails().getData()));
        assertEquals(decompressedDetails, data);
    }

    private boolean topPredictorsAreSortedAndSet(List<Predictor> predictors) {

        boolean isSorted = true;
        boolean isSet = true;
        double MinUncertaintyCoefficient = 1.0;
        for (int i = 0; i < defaultBiPredictorNum; i++) {
            Predictor p = predictors.get(i);
            double currentUncertaintyCoefficient = p.getUncertaintyCoefficient();
            if (currentUncertaintyCoefficient <= MinUncertaintyCoefficient && p.getUsedForBuyerInsights()) {
                MinUncertaintyCoefficient = currentUncertaintyCoefficient;
            } else {
                if (currentUncertaintyCoefficient > MinUncertaintyCoefficient) {
                    isSorted = false;
                }
                if (!p.getUsedForBuyerInsights()) {
                    isSet = false;
                }
                break;
            }
        }
        Predictor unSetPredictor = predictors.get(defaultBiPredictorNum);
        if (unSetPredictor.getUncertaintyCoefficient() > MinUncertaintyCoefficient) {
            isSorted = false;
        }
        if (unSetPredictor.getUsedForBuyerInsights()) {
            isSet = false;
        }
        return (isSorted && isSet);
    }

    @Test(groups = { "unit", "functional" })
    public void parseDetailsOnly() throws Exception {
        InputStream is = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/pls/service/impl/modelsummary-detailsonly.json");
        String data = new String(IOUtils.toByteArray(is));
        ModelSummary summary = modelSummaryParser.parse("modelsummary-detailsonly.json", data);

        assertEquals(summary.getName(), "PLSModel-02/18/2015 07:25:38 GMT");
        assertEquals(summary.getLookupId(), "TENANT1|Q_PLS_Modeling_TENANT1|8195dcf1-0898-4ad3-b94d-0d0f806e979e");
        assertEquals(summary.getTrainingRowCount().longValue(), 0L);
        assertEquals(summary.getTestRowCount().longValue(), 0L);
        assertEquals(summary.getTotalRowCount().longValue(), 0L);
        assertEquals(summary.getTrainingConversionCount().longValue(), 0L);
        assertEquals(summary.getTestConversionCount().longValue(), 0L);
        assertEquals(summary.getTotalConversionCount().longValue(), 0L);
        assertEquals(summary.getRocScore(), 0.0);
        assertEquals(summary.getId(), "ms__8195dcf1-0898-4ad3-b94d-0d0f806e979e-PLSModel");
        assertTrue(summary.isIncomplete(), "This model summary should be incomplete.");
        assertEquals(summary.getPredictors().size(), 0);

        String decompressedDetails = new String(CompressionUtils.decompressByteArray(summary.getDetails().getData()));
        assertEquals(decompressedDetails, data);
    }
}
