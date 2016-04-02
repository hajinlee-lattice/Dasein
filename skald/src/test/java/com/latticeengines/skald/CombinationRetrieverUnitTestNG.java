package com.latticeengines.skald;

import java.util.HashMap;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.interfaces.data.DataInterfacePublisher;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.skald.exposed.domain.ModelCombination;
import com.latticeengines.skald.exposed.domain.ModelTags;
import com.latticeengines.skald.exposed.domain.TargetedModel;

public class CombinationRetrieverUnitTestNG {
    private static class Documents {
        public static final String TestCombination_name = "TestCombination";
        public static final String TestModel1_name = "TestModel1";
        public static final String TestModel2_name = "TestModel2";

        public static final CustomerSpace Space = CamilleTestEnvironment.getCustomerSpace();

        public static final ModelTags Tags;
        public static final ModelCombination TestCombination;

        public static final ScoreDerivation TestModel1_ScoreDerivation;
        public static final DataComposition TestModel1_DataComposition;

        public static final ScoreDerivation TestModel2_ScoreDerivation;
        public static final DataComposition TestModel2_DataComposition;

        public static final ScoreDerivation TestModel1_ScoreDerivationOverride;

        private static ConfigurationController<CustomerSpaceServiceScope> dataController;
        private static DataInterfacePublisher modelPublisher;

        static {
            // Create Tags
            Tags = new ModelTags();
            HashMap<String, Integer> testModel1 = new HashMap<String, Integer>();
            testModel1.put(ModelTags.ACTIVE, 1);
            HashMap<String, Integer> testModel2 = new HashMap<String, Integer>();
            testModel2.put(ModelTags.ACTIVE, 1);
            Tags.put(TestModel1_name, testModel1);
            Tags.put(TestModel2_name, testModel2);

            // Create TestCombination
            TestCombination = new ModelCombination();
            TestCombination.add(new TargetedModel(null, TestModel1_name));
            TestCombination.add(new TargetedModel(null, TestModel2_name));

            // Create ScoreDerivation1
            TestModel1_ScoreDerivation = new ScoreDerivation(TestModel1_name, 1.0, null, null);

            // Create DataComposition1
            TestModel1_DataComposition = new DataComposition(null, null);

            // Create ScoreDerivation2
            TestModel2_ScoreDerivation = new ScoreDerivation(TestModel2_name, 1.0, null, null);

            // Create DataComposition2
            TestModel2_DataComposition = new DataComposition(null, null);

            // Create ScoreDerivation1 override
            TestModel1_ScoreDerivationOverride = new ScoreDerivation(TestModel1_name + "_overdrive!", 0.5, null, null);

            try {
                modelPublisher = new DataInterfacePublisher(DocumentConstants.MODEL_INTERFACE, Space);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static void publish() throws Exception {
            CustomerSpaceServiceScope scope = new CustomerSpaceServiceScope(Space, DocumentConstants.SERVICE_NAME);
            dataController = ConfigurationController.construct(scope);

            // Publish TestModel1
            modelPublisher.publish(new Path(String.format(DocumentConstants.SCORE_DERIVATION, TestModel1_name, 1)),
                    DocumentUtils.toRawDocument(TestModel1_ScoreDerivation));
            modelPublisher.publish(new Path(String.format(DocumentConstants.DATA_COMPOSITION, TestModel1_name, 1)),
                    DocumentUtils.toRawDocument(TestModel1_DataComposition));

            // Publish TestModel2
            modelPublisher.publish(new Path(String.format(DocumentConstants.SCORE_DERIVATION, TestModel2_name, 1)),
                    DocumentUtils.toRawDocument(TestModel2_ScoreDerivation));
            modelPublisher.publish(new Path(String.format(DocumentConstants.DATA_COMPOSITION, TestModel2_name, 1)),
                    DocumentUtils.toRawDocument(TestModel2_DataComposition));

            // Publish TestCombination
            // TODO Camille should provide a mechanism to automatically create
            // parents through this interface.
            dataController.create(new Path("/Combinations"), new Document());
            dataController.create(new Path(String.format(DocumentConstants.COMBINATION, TestCombination_name)),
                    DocumentUtils.toRawDocument(TestCombination));

            // Publish ModelTags
            dataController.create(new Path(DocumentConstants.MODEL_TAGS), DocumentUtils.toRawDocument(Tags));
        }

        public static void publishScoreOverride() throws Exception {
            dataController.create(
                    new Path(String.format(DocumentConstants.SCORE_DERIVATION_OVERRIDE, TestModel1_name, 1)),
                    DocumentUtils.toRawDocument(TestModel1_ScoreDerivationOverride));
        }
    }

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();

        SkaldBootstrapper.register();
        CustomerSpaceServiceBootstrapManager.reset(DocumentConstants.SERVICE_NAME, Documents.Space);

        Documents.publish();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testGetCombination() {
        CombinationRetriever retriever = new CombinationRetriever();
        List<CombinationElement> combination = retriever.getCombination(Documents.Space,
                Documents.TestCombination_name, ModelTags.ACTIVE);
        Assert.assertEquals(combination.size(), 2);
        boolean model1Found = false;
        boolean model2Found = false;
        for (CombinationElement element : combination) {
            if (element.model.name.equals(Documents.TestModel1_name)) {
                Assert.assertEquals(element.data, Documents.TestModel1_DataComposition);
                Assert.assertEquals(element.derivation, Documents.TestModel1_ScoreDerivation);
                model1Found = true;
            } else if (element.model.name.equals(Documents.TestModel2_name)) {
                Assert.assertEquals(element.data, Documents.TestModel2_DataComposition);
                Assert.assertEquals(element.derivation, Documents.TestModel2_ScoreDerivation);
                model2Found = true;
            }
        }
        Assert.assertTrue(model1Found && model2Found);
    }

    @Test(groups = "unit")
    public void testScoreOverride() throws Exception {
        // Override ScoreDerivation for TestModel1
        Documents.publishScoreOverride();

        CombinationRetriever retriever = new CombinationRetriever();
        List<CombinationElement> combination = retriever.getCombination(Documents.Space,
                Documents.TestCombination_name, ModelTags.ACTIVE);
        Assert.assertEquals(combination.size(), 2);
        boolean model1Found = false;
        boolean model2Found = false;
        for (CombinationElement element : combination) {
            if (element.model.name.equals(Documents.TestModel1_name)) {
                Assert.assertEquals(element.data, Documents.TestModel1_DataComposition);
                Assert.assertEquals(element.derivation, Documents.TestModel1_ScoreDerivationOverride);
                model1Found = true;
            } else if (element.model.name.equals(Documents.TestModel2_name)) {
                Assert.assertEquals(element.data, Documents.TestModel2_DataComposition);
                Assert.assertEquals(element.derivation, Documents.TestModel2_ScoreDerivation);
                model2Found = true;
            }
        }
        Assert.assertTrue(model1Found && model2Found);
    }

    @Test(groups = "unit", expectedExceptions = Exception.class)
    public void testNoSuchTag() throws Exception {
        CombinationRetriever retriever = new CombinationRetriever();
        retriever.getCombination(Documents.Space, Documents.TestCombination_name, "UnknownTag");
    }

    @Test(groups = "unit", expectedExceptions = Exception.class)
    public void testNoSuchCombination() throws Exception {
        CombinationRetriever retriever = new CombinationRetriever();
        retriever.getCombination(Documents.Space, "UnknownCombination", ModelTags.ACTIVE);
    }
}
