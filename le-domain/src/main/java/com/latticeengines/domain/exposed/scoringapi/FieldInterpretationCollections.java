package com.latticeengines.domain.exposed.scoringapi;

import java.util.EnumSet;

/**
 * Exposes different combination of FieldInterpretation collections and its validation configurations without
 * modifying the source enum.
 */
public class FieldInterpretationCollections {

	/**
	 * Matching Field Validation Expression if Fuzzy Matching feature is enabled
	 */
	public static final String FUZZY_MATCH_VALIDATION_EXPRESSION = String.format("( %s || %s || %s || %s ) && %s ",
			FieldInterpretation.Email, 
			FieldInterpretation.Website,
			FieldInterpretation.CompanyName,
			FieldInterpretation.DUNS,
			FieldInterpretation.Id);
	
	/**
	 * Matching Field Validation Expression if Fuzzy Matching feature is not enabled
	 */
	public static final String NON_FUZZY_MATCH_VALIDATION_EXPRESSION = String.format("( %s || %s ) && %s ",
			FieldInterpretation.Email, 
			FieldInterpretation.Website, 
			FieldInterpretation.Id);
	
	/**
	 * Matching Field Validation Expression if Model is RTS based
	 */
	public static final String RTS_MODEL_VALIDATION_EXPRESSION = NON_FUZZY_MATCH_VALIDATION_EXPRESSION;
	
    /**
     * Attribute collection to represent the Primary fields, that are used in
     * multiple places 1. Model fields needs to be marked as Primary for
     * required mappings 2. Scoring API accepts only these attributes for Fuzzy
     * matching 3. Account Lookup accepts only these attributes for Fuzzy
     * matching
     */
    public static final EnumSet<FieldInterpretation> PrimaryMatchingFields = (EnumSet<FieldInterpretation>) EnumSet.of(
    		FieldInterpretation.Id, 
            FieldInterpretation.CompanyName,
            FieldInterpretation.Email, 
            FieldInterpretation.Website,
            FieldInterpretation.DUNS,
            FieldInterpretation.City,
            FieldInterpretation.State, 
            FieldInterpretation.Country, 
            FieldInterpretation.PostalCode,
            FieldInterpretation.PhoneNumber);
}
