package com.latticeengines.domain.exposed.pls;

public enum ProvenancePropertyName {
    IsOneLeadPerDomain("Is_One_Lead_Per_Domain", Boolean.class), //
    ExcludePropdataColumns("Exclude_Propdata_Columns", Boolean.class), //
    ExcludePublicDomains("Exclude_Public_Domains", Boolean.class), //
    TransformationGroupName("Transformation_Group_Name", String.class), //
    TrainingFilePath("Training_File_Path", String.class), //
    WorkflowJobId("Workflow_Job_Id", Long.class), //
    IsV2ProfilingEnabled("Is_V2_Profiling_Enabled", Boolean.class), //
    ConflictWithOptionalRules("Conflict_With_Optional_Rules", Boolean.class), //
    FuzzyMatchingEnabled("Fuzzy_Matching_Enabled", Boolean.class), //
    RefineAndCloneParentModelId("Refine_And_Clone_Parent_Model_Id", String.class);

    private String name;
    private Class<?> type;

    ProvenancePropertyName(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public Class<?> getType() {
        return this.type;
    }
}
