'use strict';

describe('MetadataUtility Tests', function () {

    var metadataUtil,
        sortUtil,
        sourceMetadata,
        talkingPointMetadata;
    
    beforeEach(function () {
        module('mainApp.appCommon.utilities.SortUtility');
        module('mainApp.appCommon.utilities.MetadataUtility');
        module('test.testData.MetadataTestDataService');
        
        inject(['MetadataUtility', 'SortUtility', 'MetadataTestDataService',
            function (MetadataUtility, SortUtility, MetadataTestDataService) {
                metadataUtil = MetadataUtility;
                sortUtil = SortUtility;
                sourceMetadata = MetadataTestDataService.GetSampleMetadata();
            }
        ]);
        
        talkingPointMetadata = metadataUtil.GetNotionMetadata(
            "DanteTalkingPoint", sourceMetadata);
    });
    
    describe('GetNotionMetadata given invalid parameters', function () {
        var metadata;
        
        it('should return null for a null notion name', function () {
            metadata = metadataUtil.GetNotionMetadata(null, sourceMetadata);
            expect(metadata).toBe(null);
        });
        
        it('should return null for an empty string notion name', function () {
            metadata = metadataUtil.GetNotionMetadata("", sourceMetadata);
            expect(metadata).toBe(null);
        });
        
        it('should return null for a null source metadata', function () {
            metadata = metadataUtil.GetNotionMetadata("Account.Lead", null);
            expect(metadata).toBe(null);
        });
        
        it('should return null if the notion name does not exist in the source metadata', function () {
            metadata = metadataUtil.GetNotionMetadata("Apple", sourceMetadata);
            expect(metadata).toBe(null);
        });
    });
    
    describe('GetNotionMetadata given valid parameters', function () {
        var notionName,
            actMetadata;
            
        it('should return correct metadata for DanteLead as a notion name', function () {
            notionName = "DanteLead";
            actMetadata = metadataUtil.GetNotionMetadata(notionName, sourceMetadata);
            expect(actMetadata.Name).toEqual(notionName);
        });
        
        it('should return correct metadata for TalkingPoint as a notion name', function () {
            notionName = "DanteTalkingPoint";
            actMetadata = metadataUtil.GetNotionMetadata(notionName, sourceMetadata);
            expect(actMetadata.Name).toEqual(notionName);
        });
    });
    
    describe('GetNotionProperty given invalid parameters', function () {
        var actMetadata;
                    
        it('should return null given a null property name', function () {
            actMetadata = metadataUtil.GetNotionProperty(null, talkingPointMetadata);
            expect(actMetadata).toEqual(null);
        });
        
        it('should return null given an empty property name', function () {
            actMetadata = metadataUtil.GetNotionProperty("", talkingPointMetadata);
            expect(actMetadata).toEqual(null);
        });
        
        it('should return null given a property name that does not exist for the notion', function () {
            var metadataWithoutTitle = metadataUtil.GetNotionMetadata("DanteLead", sourceMetadata);
            actMetadata = metadataUtil.GetNotionProperty("Title", metadataWithoutTitle);
            expect(actMetadata).toEqual(null);
        });
        
        it('should return null given a null source metadata', function () {
            actMetadata = metadataUtil.GetNotionProperty("Title", null);
            expect(actMetadata).toEqual(null);
        });
    });
    
    describe('GetNotionProperty given valid parameters', function () {
        it('should return the correct metadata given a valid property', function () {
            var expMetadata = {
                "Name": "Title",
                "DisplayNameKey": "DANTE_TALKING_POINTS_TITLE_LABEL",
                "DescriptionKey": "DANTE_TALKING_POINTS_TITLE_DESCRIPTION",
                "PropertyTypeString": "String",
                "PropertyType": 5,
                "Interpretation": 0
            };
            var actMetadata = metadataUtil.GetNotionProperty("Title", talkingPointMetadata);
            expect(actMetadata).toEqual(expMetadata);
        });
    });
    
    describe('GetNotionAssociationMetadata given invalid parameters', function () {
        var actMetadata;
        
        it('should return null given an invalid Notion and a valid association Notion', function () {
            actMetadata = metadataUtil.GetNotionAssociationMetadata(
                "FakeNotion", "DanteTalkingPoint", sourceMetadata);
            expect(actMetadata).toEqual(null);
        });
        
        it('should return null given a valid Notion and an invalid association Notion', function () {
            actMetadata = metadataUtil.GetNotionAssociationMetadata(
                "DanteLead", "FakeAssociationNotion", sourceMetadata);
            expect(actMetadata).toEqual(null);
        });
    });
    
    describe('GetNotionAssociationMetadata given valid parameters', function () {
        it ('should return the association metadata given a valid Notion and association Notion', function () {
            var expMetadata = {
                "Cardinality": 1,
                "Name": "TalkingPoints",
                "TargetNotion": "DanteTalkingPoint",
                "SourceKeyName": "PreLead_ID",
                "TargetKeyName": "TalkingPoints_ID",
                "IsWeakAssociation": false
            };
            var actMetadata = metadataUtil.GetNotionAssociationMetadata(
                "DanteLead", "DanteTalkingPoint", sourceMetadata);
            expect(actMetadata).toEqual(expMetadata);
        });
    });
    
    describe('GetCompareFunction given invalid parameters', function () {
        it('should return the default compare function given an invalid Property Type', function () {
            var actFunc = metadataUtil.GetCompareFunction("FOOBAR");
            var expFunc = sortUtil.DefaultCompare;
            expect(actFunc).toBe(expFunc);
        });
    });
    
    describe('GetCompareFunction given valid parameters', function () {    
        it('should return the default compare function given a valid Property Type', function () {
            var actFunc = metadataUtil.GetCompareFunction(
                metadataUtil.PropertyType.PROBABILITY);
            var expFunc = sortUtil.DefaultCompare;
            expect(actFunc).toBe(expFunc);
        });
    });
    
});