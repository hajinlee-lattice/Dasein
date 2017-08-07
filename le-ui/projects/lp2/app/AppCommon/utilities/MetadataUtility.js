angular.module('mainApp.appCommon.utilities.MetadataUtility', [
    'mainApp.appCommon.utilities.SortUtility'
])
.service('MetadataUtility', function (SortUtility) {
    
    //Constants
    this.PropertyType = {
        INTEGER: "Int",
        INTEGER64: "Int64",
        DECIMAL: "Decimal",
        DOUBLE: "Double",
        BOOL: "Bool",
        STRING: "String",
        DATE_TIME_OFFSET: "DateTimeOffset",
        CURRENCY: "Currency",
        PROBABILITY: "Probability",
        PERCENTAGE: "Percentage",
        EPOCH_TIME: "EpochTime"
    };
    
    this.ApplicationContext = {
        Account: "LatticeForAccounts",
        Lead: "LatticeForLeads"
    };
    
    this.NotionNames = {
        Lead: "DanteLead"
    };
    
    this.GetNotionMetadata = function (notionName, metadata) {
        if (metadata == null || metadata.Notions == null || metadata.Notions.length === 0 || notionName == null || notionName === "") {
            return null;
        }
        for (var i = 0; i < metadata.Notions.length; i++) {
            var notion = metadata.Notions[i];
            if (notionName == notion.Key) {
                return notion.Value;
            }
        }
        return null;
    };
    
    this.GetNotionAssociationMetadata = function (notionName, associationNotionName, rootMetadata) {
        if (rootMetadata == null || notionName == null || associationNotionName == null || associationNotionName === "") {
            return null;
        }
        
        var metadata = this.GetNotionMetadata(notionName, rootMetadata);
        
        if (metadata == null || metadata.Associations == null) {
            return null;
        }
        
        for (var i = 0; i < metadata.Associations.length; i++) {
            var metadataAssociation = metadata.Associations[i];
            if (associationNotionName == metadataAssociation.TargetNotion) {
                return metadataAssociation;
            }
            
        }
        return null;
    };
    
    // Must pass in the relevant metadata object in order to get the correct property
    // e.g. Must pass in the DanteLead notion to get it's Name property
    this.GetNotionProperty = function (propertyName, notionMetadata) {
        if (notionMetadata == null || notionMetadata.Properties == null ||
            propertyName == null || propertyName === "") {
            return null;
        }

        for (var i = 0; i < notionMetadata.Properties.length; i++) {
            var metadataProperty = notionMetadata.Properties[i];
            if (propertyName == metadataProperty.Name) {
                return metadataProperty;
            }
            
        }
        return null;
    };

    this.GetCompareFunction = function (propertyTypeString) {
        switch (propertyTypeString) {
            // If you need to use custom compare functions 
            // for certain types (MetadataConstants.PropertyType),
            // put the cases here: 
            default:
                return SortUtility.DefaultCompare;
        }
    };
});