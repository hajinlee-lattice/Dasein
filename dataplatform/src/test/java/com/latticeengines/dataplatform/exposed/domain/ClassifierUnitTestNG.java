package com.latticeengines.dataplatform.exposed.domain;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;

import org.testng.annotations.Test;

import com.latticeengines.dataplatform.util.JsonHelper;

public class ClassifierUnitTestNG {
	
	@Test(groups="unit")
	public void testSerialize() throws Exception {
		Classifier classifier = new Classifier();
		classifier.setName("NeuralNetworkClassifier");
		DataSchema schema = new DataSchema();
		classifier.setSchema(schema);
		schema.setName("IrisDataSet");
		schema.setType("record");
		Field sepalLength = new Field();
		sepalLength.setName("sepal_length");
		sepalLength.setType(Arrays.<String>asList(new String[] { "float", "0.0" }));
		Field sepalWidth = new Field();
		sepalWidth.setName("sepal_width");
		sepalWidth.setType(Arrays.<String>asList(new String[] { "float", "0.0" }));
		Field petalLength = new Field();
		petalLength.setName("petal_length");
		petalLength.setType(Arrays.<String>asList(new String[] { "float", "0.0" }));
		Field petalWidth = new Field();
		petalWidth.setName("petal_width");
		petalWidth.setType(Arrays.<String>asList(new String[] { "float", "0.0" }));
		Field category = new Field();
		category.setName("category");
		category.setType(Arrays.<String>asList(new String[] { "string", "null" }));
		
		schema.addField(sepalLength);
		schema.addField(sepalWidth);
		schema.addField(petalLength);
		schema.addField(petalWidth);
		schema.addField(category);
		
		classifier.addFeature(sepalLength);
		classifier.addFeature(sepalWidth);
		classifier.addFeature(petalLength);
		classifier.addTarget(category);
		
		String jsonString = classifier.toString();
		Classifier deserializedClassifier = JsonHelper.deserialize(jsonString, Classifier.class);
		assertEquals(deserializedClassifier.toString(), jsonString);

	}
}

	