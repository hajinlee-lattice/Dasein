package com.latticeengines.domain.exposed.metadata;

public final class ColumnMetadataKey {

    protected ColumnMetadataKey() {
        throw new UnsupportedOperationException();
    }

    public static final String AttrName = "AttrName";
    public static final String DisplayName = "DisplayName";
    public static final String SecondaryDisplayName = "SecondaryDisplayName";
    public static final String Description = "Description";

    public static final String Entity = "Entity";
    public static final String State = "State";

    public static final String Category = "Category";
    public static final String Subcategory = "Subcategory";
    public static final String SecondarySubCategoryDisplayName = "SubcategoryAltName";

    public static final String ApprovedUsage = "ApprovedUsage";

}
