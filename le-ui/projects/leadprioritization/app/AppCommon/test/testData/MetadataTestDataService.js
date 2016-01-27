angular.module('test.testData.MetadataTestDataService', [
    'mainApp.appCommon.utilities.MetadataUtility'])
.service('MetadataTestDataService', function(MetadataUtility) {

    this.GetSampleMetadata = function () {
        return {
            Notions: [{
                Key: "DanteAccount",
                Value : {
                    Name: "DanteAccount",
                    DisplayNameKey: "DANTE_ACCOUNT_LABEL",
                    DescriptionKey: "DANTE_ACCOUNT_DESCRIPTION",
                    Properties: [{
                            Name: "Name",
                            DisplayNameKey: "DANTE_ACCOUNT_NAME_LABEL",
                            DescriptionKey: "DANTE_ACCOUNT_LABEL_DESCRIPTION",
                            PropertyTypeString: MetadataUtility.PropertyType.STRING,
                            PropertyType: 5,
                            Interpretation: 0
                        }, {
                            Name: "Account_ID",
                            DisplayNameKey: "DANTE_ACCOUNT_ID_LABEL",
                            DescriptionKey: "DANTE_ACCOUNT_ID_DESCRIPTION",
                            PropertyTypeString: MetadataUtility.PropertyType.STRING,
                            PropertyType: 5,
                            Interpretation: 1
                        }
                    ],
                    Associations : [{
                            Cardinality: 1,
                            Name: "Plays",
                            TargetNotion: "DanteLead",
                            SourceKeyName: "AccountID",
                            TargetKeyName: "PreLeadID",
                            IsWeakAssociation: false
                        }
                    ]}
                }, {
                    Key: "DanteLead",
                    Value : {
                        Name: "DanteLead",
                        DisplayNameKey: "DANTE_PRELEAD_LABEL",
                        DescriptionKey: "DANTE_PRELEAD_DESCRIPTION",
                        Properties: [{
                                Name: "Play_Name",
                                DisplayNameKey: "DANTE_PRELEAD_NAME_LABEL",
                                DescriptionKey: "DANTE_PRELEAD_NAME_DESCRIPTION",
                                PropertyTypeString: MetadataUtility.PropertyType.STRING,
                                PropertyType: 5,
                                Interpretation: 0
                            }, {
                                Name: "Play_Type",
                                DisplayNameKey: "DANTE_PRELEAD_TYPE_LABEL",
                                DescriptionKey: "DANTE_PRELEAD_TYPE_DESCRIPTION",
                                PropertyTypeString: MetadataUtility.PropertyType.STRING,
                                PropertyType: 5,
                                Interpretation: 0
                            }, {
                                Name: "Play_Description",
                                DisplayNameKey: "DANTE_PRELEAD_DESCRIPTION_LABEL",
                                DescriptionKey: "DANTE_PRELEAD_DESCRIPTION_DESCRIPTION",
                                PropertyTypeString: MetadataUtility.PropertyType.STRING,
                                PropertyType: 5,
                                Interpretation: 0
                            }, {
                                Name: "Likelihood",
                                DisplayNameKey: "DANTE_PRELEAD_LIKELIHOOD_LABEL",
                                DescriptionKey: "DANTE_PRELEAD_LIKELIHOOD_DESCRIPTION",
                                PropertyType: 8,
                                Interpretation: 0
                            }, {
                                Name: "Expected_Value",
                                DisplayNameKey: "DANTE_PRELEAD_EXPECTED_VALUE_LABEL",
                                DescriptionKey: "DANTE_PRELEAD_EXPECTED_VALUE_DESCRIPTION",
                                PropertyType: 7,
                                Interpretation: 0
                            }, {
                                Name: "Lift",
                                DisplayNameKey: "DANTE_PRELEAD_LIFT_LABEL",
                                DescriptionKey: "DANTE_PRELEAD_LIFT_DESCRIPTION",
                                PropertyType: 0,
                                Interpretation: 0
                            }, {
                                Name: "Last_Launch_Date",
                                DisplayNameKey: "DANTE_PRELEAD_LAST_LAUNCH_DATE_LABEL",
                                DescriptionKey: "DANTE_PRELEAD_LAST_LAUNCH_DATE_DESCRIPTION",
                                PropertyType: 6,
                                Interpretation: 0
                            }, {
                                Name: "PreLead_ID",
                                DisplayNameKey: "DANTE_PRELEAD_ID_LABEL",
                                DescriptionKey: "DANTE_PRELEAD_ID_DESCRIPTION",
                                PropertyTypeString: MetadataUtility.PropertyType.STRING,
                                PropertyType: 5,
                                Interpretation: 1
                            }, {
                                Name: "Account_ID",
                                DisplayNameKey: "DANTE_PRELEAD_ACCOUNT_ID_LABEL",
                                DescriptionKey: "DANTE_PRELEAD_ACCOUNT_ID_DESCRIPTION",
                                PropertyTypeString: MetadataUtility.PropertyType.STRING,
                                PropertyType: 5,
                                Interpretation: 2
                            }
                        ],
                        Associations: [{
                            Cardinality: 1,
                            Name: "TalkingPoints",
                            TargetNotion: "DanteTalkingPoint",
                            SourceKeyName: "PreLead_ID",
                            TargetKeyName: "TalkingPoints_ID",
                            IsWeakAssociation: false
                        }]
                    }
                }, {
                Key: "DanteTalkingPoint",
                Value : {
                    Name: "DanteTalkingPoint",
                    DisplayNameKey: "DANTE_TALKING_POINTS_LABEL",
                    DescriptionKey: "DANTE_TALKING_POINTS_DESCRIPTION",
                    Properties: [{
                            Name: "Title",
                            DisplayNameKey: "DANTE_TALKING_POINTS_TITLE_LABEL",
                            DescriptionKey: "DANTE_TALKING_POINTS_TITLE_DESCRIPTION",
                            PropertyTypeString: MetadataUtility.PropertyType.STRING,
                            PropertyType: 5,
                            Interpretation: 0
                        }, {
                            Name: "Content",
                            DisplayNameKey: "DANTE_TALKING_POINTS_CONTENT_LABEL",
                            DescriptionKey: "DANTE_TALKING_POINTS_CONTENT_DESCRIPTION",
                            PropertyTypeString: MetadataUtility.PropertyType.STRING,
                            PropertyType: 5,
                            Interpretation: 0
                        }, {
                            Name: "Offset",
                            DisplayNameKey: "DANTE_TALKING_POINTS_OFFSET_LABEL",
                            DescriptionKey: "DANTE_TALKING_POINTS_OFFSET_DESCRIPTION",
                            PropertyType: 3,
                            Interpretation: 0
                        },{
                            Name: "TalkingPoints_ID",
                            DisplayNameKey: "DANTE_TALKING_POINTS_ID_LABEL",
                            DescriptionKey: "DANTE_TALKING_POINTS_ID_DESCRIPTION",
                            PropertyTypeString: MetadataUtility.PropertyType.STRING,
                            PropertyType: 5,
                            Interpretation: 1
                        }, {
                            Name: "Play_ID",
                            DisplayNameKey: "DANTE_TALKING_POINTS_PRELEAD_ID_LABEL",
                            DescriptionKey: "DANTE_TALKING_POINTS_PRELEAD_ID_DESCRIPTION",
                            PropertyTypeString: MetadataUtility.PropertyType.STRING,
                            PropertyType: 5,
                            Interpretation: 2
                        }
                    ],
                    Associations: []
                }
            }]
        };
    }; //end GetSampleMetadata()

});