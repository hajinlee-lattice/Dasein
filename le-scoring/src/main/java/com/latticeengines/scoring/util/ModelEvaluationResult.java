package com.latticeengines.scoring.util;

public class ModelEvaluationResult {

	public ModelEvaluationResult()
	{
	}
	
	public ModelEvaluationResult(Double scoreVal, Long integerScoreVal, String bucketNameVal, String modelNameVal, 
			Double probabilityVal, Double liftVal, Long percentileVal) {
		score = scoreVal;
		integerScore = integerScoreVal;
		bucketName = bucketNameVal;
		modelName = modelNameVal;
		probability = probabilityVal;
		lift = liftVal;
		percentile = percentileVal;
	}
	
	public Double score;
	public Long integerScore;
	public String bucketName;
	public String modelName;
	public Double probability;
	public Double lift;
	public Long percentile;
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("score:" + score + "| ");
		sb.append("integerScore:" + integerScore + "| ");
		sb.append("bucketName:" + bucketName + "| ");
		sb.append("modelName:" + modelName + "| ");
		sb.append("probability:" + probability + "| ");
		sb.append("lift:" + lift + "| ");
		sb.append("percentile:" + percentile + "| ");
		return sb.toString();
	}
}
