angular.module('test.testData.AnalyticAttributeTestDataService', [
    'mainApp.appCommon.utilities.AnalyticAttributeUtility'])
.service('AnalyticAttributeTestDataService', function (AnalyticAttributeUtility) {

    this.GetSampleModelSummaryData = function () {
        return {
            ExternalID: "119a3895-ff31-474c-9103-70bfaf119964",
            AttributeMetadata: [
                {
                    ColumnName: "Attr1",
                    ApprovedUsage: [
                        "ModelAndAllInsights"
                    ],
                    Description: "Description1",
                    DisplayName: "DisplayName1",
                    Tags: [
                        "External"
                    ],
                    DataType: "String"
                },
                {
                    ColumnName: "Attr2",
                    ApprovedUsage: [
                        "Model"
                    ],
                    Description: "Description2",
                    DisplayName: "DisplayName2",
                    Tags: [
                        "External"
                    ],
                    DataType: "Int"
                },
                {
                    ColumnName: "Attr3",
                    ApprovedUsage: [
                        "None"
                    ],
                    Description: "Description3",
                    DisplayName: "DisplayName3",
                    Tags: [
                        "External"
                    ],
                    DataType: "Int"
                },
                {
                    ColumnName: "Attr4",
                    ApprovedUsage: [
                        "ModelAndAllInsights"
                    ],
                    Description: "Description4",
                    DisplayName: "DisplayName4",
                    Tags: [
                        "Internal"
                    ],
                    DataType: "Int"
                }
            ],
            Summary: {
                ModelID: "119a3895-ff31-474c-9103-70bfaf119964",
                Predictors: [
                    {
                        Name: "Attr1",
                        Elements: [
                            {
                                ColumnName: "Attr1Name1",
                                Values: [
                                    "Attr1Value1"
                                ],
                                LowerInclusive: null,
                                UpperExclusive: null
                            },
                            {
                                ColumnName: "Attr1Name2",
                                Values: [
                                    "Attr1Value2"
                                ],
                                LowerInclusive: null,
                                UpperExclusive: null
                            },
                            {
                                ColumnName: "Attr1Name3",
                                Values: [
                                    "Attr1Value3"
                                ],
                                LowerInclusive: null,
                                UpperExclusive: null
                            }
                        ]
                    },
                    {
                        Name: "Attr4",
                        Elements: [
                            {
                                ColumnName: "Attr4Name1",
                                Values: [],
                                LowerInclusive: null,
                                UpperExclusive: 40
                            },
                            {
                                ColumnName: "Attr4Name2",
                                Values: [],
                                LowerInclusive: 40,
                                UpperExclusive: 60
                            },
                            {
                                ColumnName: "Attr4Name3",
                                Values: [],
                                LowerInclusive: 60,
                                UpperExclusive: 80
                            },
                            {
                                ColumnName: "Attr4Name4",
                                Values: [],
                                LowerInclusive: 80,
                                UpperExclusive: null
                            }
                        ]
                    }
                ]
            }
            
        };
    };

});