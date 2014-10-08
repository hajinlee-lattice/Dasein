package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.Field;

public class ClassifierUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() throws Exception {
        Classifier classifier = new Classifier();
        classifier.setName("NeuralNetworkClassifier");
        classifier.setSchemaHdfsPath("/datascientist1/iris.json");
        Field sepalLength = new Field();
        sepalLength.setName("sepal_length");
        sepalLength.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field sepalWidth = new Field();
        sepalWidth.setName("sepal_width");
        sepalWidth.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field petalLength = new Field();
        petalLength.setName("petal_length");
        petalLength.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field petalWidth = new Field();
        petalWidth.setName("petal_width");
        petalWidth.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field category = new Field();
        category.setName("category");
        category.setType(Arrays.<String> asList(new String[] { "string", "null" }));

        classifier.addFeature(sepalLength.getName());
        classifier.addFeature(sepalWidth.getName());
        classifier.addFeature(petalLength.getName());
        classifier.addTarget(category.getName());

        classifier.setTrainingDataHdfsPath("/training/nn_train.dat");
        classifier.setTestDataHdfsPath("/test/nn_test.dat");
        classifier.setPythonScriptHdfsPath("/datascientist1/nn_train.py");
        classifier.setModelHdfsDir("/datascientist1/result");
        classifier.setDataProfileHdfsPath("/datascientist1/a.avro");

        String jsonString = classifier.toString();
        System.out.println(jsonString);
        Classifier deserializedClassifier = JsonUtils.deserialize(jsonString, Classifier.class);
        assertEquals(deserializedClassifier.toString(), jsonString);

    }

}
