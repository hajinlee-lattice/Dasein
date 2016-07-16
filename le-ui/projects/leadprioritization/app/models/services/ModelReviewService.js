angular
.module('lp.models.review', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ModelReviewStore', function($q, ModelReviewService) {
    var ModelReviewStore = this;
    this.reviewDataMap = {};

    this.AddDataRule = function(modelId, dataRule) {
        var reviewData = this.reviewDataMap[modelId];
        for (var i in reviewData.dataRules) {
            if (dataRule.name == reviewData.dataRules[i].name) {
              ModelReviewStore.RemoveDataRule(modelId, dataRule.name);
            }
        }
        reviewData.dataRules.push(dataRule);
    };

    this.GetDataRules = function(modelId) {
        return this.reviewDataMap[modelId].dataRules;
    };

    this.RemoveDataRule = function(modelId, name) {
        var reviewData = this.reviewDataMap[modelId];
        for (var i in reviewData.dataRules) {
            if (reviewData.dataRules[i].name == name) {
                reviewData.dataRules.splice(i, 1);
            }
        }
    };

    this.SetReviewData = function(modelId, reviewData) {
        this.reviewDataMap[modelId] = reviewData;
    };

    this.GetReviewData = function(modelId, eventTableName) {
        var deferred = $q.defer(),
            reviewData = this.reviewDataMap[modelId];

        if (typeof reviewData == 'object') {
            deferred.resolve(reviewData);
        } else {
             ModelReviewService.GetModelReviewData(modelId, eventTableName).then(function(result) {
                 if (result.Success === true) {
                     var modelReviewData = result.Result;
                     ModelReviewStore.SetReviewData(modelId, modelReviewData);
                     deferred.resolve(result.Result);
                 }
             });
        }
        return deferred.promise;
    };

    this.ResetReviewData = function() {
        this.reviewDataMap = {};
    };
})
.service('ModelReviewService', function($q, $http, ResourceUtility, ServiceErrorUtility) {
    this.GetModelReviewData = function(modelId, eventTableName) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/models/reviewmodel/mocked/' + modelId + '/' + eventTableName,
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            if (data == null || !data.Success) {
                if (data && data.Errors.length > 0) {
                    var errors = data.Errors.join('\n');
                }
                result = {
                    Success: false,
                    ResultErrors: errors || ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
                    Result: null
                };
            } else {
                result = {
                    Success: true,
                    ResultErrors: data.Errors,
                    Result: {
                              "dataRules": [
                                {
                                  "name": "UniqueValueCountDS",
                                  "displayName": "Unique Value Count",
                                  "description": "Unique value count in column - Integrated from Profiling",
                                  "columnsToRemediate": [],
                                  "properties": {},
                                  "enabled": false,
                                  "frozenEnablement": false
                                },
                                {
                                  "name": "PopulatedRowCountDS",
                                  "displayName": "Populated Row Count",
                                  "description": "Populated Row Count - Integrated from Profiling (certain value exceeds x%)",
                                  "columnsToRemediate": [],
                                  "properties": {},
                                  "enabled": false,
                                  "frozenEnablement": false
                                },
                                {
                                  "name": "OverlyPredictiveDS",
                                  "displayName": "Overly Predictive Columns",
                                  "description": "Overly predictive single category / value range",
                                  "columnsToRemediate": [],
                                  "properties": {},
                                  "enabled": false,
                                  "frozenEnablement": false
                                },
                                {
                                  "name": "LowCoverageDS",
                                  "displayName": "Low Coverage",
                                  "description": "Low coverage (empty exceeds x%)",
                                  "columnsToRemediate": [],
                                  "properties": {},
                                  "enabled": false,
                                  "frozenEnablement": false
                                },
                                {
                                  "name": "NullIssueDS",
                                  "displayName": "Positively Predictive Nulls",
                                  "description": "Positively predictive nulls",
                                  "columnsToRemediate": [],
                                  "properties": {},
                                  "enabled": false,
                                  "frozenEnablement": false
                                },
                                {
                                  "name": "HighlyPredictiveSmallPopulationDS",
                                  "displayName": "High Predictive Low Population",
                                  "description": "High predictive, low population",
                                  "columnsToRemediate": [],
                                  "properties": {},
                                  "enabled": false,
                                  "frozenEnablement": false
                                }
                              ],
                              "ruleNameToColumnRuleResults": {
                                "OverlyPredictiveDS": {
                                  "flaggedItemCount": 9,
                                  "dataRuleName": "OverlyPredictiveDS",
                                  "tenant": {
                                    "Identifier": "LETest1468546401920.LETest1468546401920.Production",
                                    "DisplayName": "LETest1468546401920_LP3",
                                    "UIVersion": "3.0"
                                  },
                                  "modelId": "ms__40ab80d5-3c97-4394-a540-929e36cf2fd1-SelfServ",
                                  "flaggedColumnNames": [
                                    "BusinessTechnologiesJavascript",
                                    "AlexaGBUsers",
                                    "AlexaAUUsers",
                                    "Some_Column",
                                    "AlexaGBRank",
                                    "AlexaAURank",
                                    "AlexaAUPageViews",
                                    "AlexaGBPageViews",
                                    "FundingFiscalYear"
                                  ]
                                },
                                "PopulatedRowCountDS": {
                                  "flaggedItemCount": 7,
                                  "dataRuleName": "PopulatedRowCountDS",
                                  "tenant": {
                                    "Identifier": "LETest1468546401920.LETest1468546401920.Production",
                                    "DisplayName": "LETest1468546401920_LP3",
                                    "UIVersion": "3.0"
                                  },
                                  "modelId": "ms__40ab80d5-3c97-4394-a540-929e36cf2fd1-SelfServ",
                                  "flaggedColumnNames": [
                                    "CloudTechnologies_CaseManagement",
                                    "FeatureTermOrderHistory",
                                    "CloudTechnologies_ProjectMgnt_Two",
                                    "Source_BusinessCountry",
                                    "CloudTechnologies_ChangeManagement",
                                    "FundingStage1",
                                    "CloudTechnologies_SustainabilityGreenEnterprise"
                                  ]
                                },
                                "NullIssueDS": {
                                  "flaggedItemCount": 0,
                                  "dataRuleName": "NullIssueDS",
                                  "tenant": {
                                    "Identifier": "LETest1468546401920.LETest1468546401920.Production",
                                    "DisplayName": "LETest1468546401920_LP3",
                                    "UIVersion": "3.0"
                                  },
                                  "modelId": "ms__40ab80d5-3c97-4394-a540-929e36cf2fd1-SelfServ",
                                  "flaggedColumnNames": []
                                },
                                "LowCoverageDS": {
                                  "flaggedItemCount": 29,
                                  "dataRuleName": "LowCoverageDS",
                                  "tenant": {
                                    "Identifier": "LETest1468546401920.LETest1468546401920.Production",
                                    "DisplayName": "LETest1468546401920_LP3",
                                    "UIVersion": "3.0"
                                  },
                                  "modelId": "ms__40ab80d5-3c97-4394-a540-929e36cf2fd1-SelfServ",
                                  "flaggedColumnNames": [
                                    "AssetsStartOfYear",
                                    "Source_BusinessCountry",
                                    "AlexaCAPageViews",
                                    "FundingStage1",
                                    "AlexaGBUsers",
                                    "AlexaAUUsers",
                                    "FundingTotalContractedAmount",
                                    "RetirementAssetsEOY",
                                    "FundingFinanceRound",
                                    "RetirementAssetsYOY",
                                    "FundingAmount",
                                    "TotalParticipantsSOY",
                                    "AlexaCARank",
                                    "AwardYear",
                                    "Some_Column",
                                    "AlexaCAUsers",
                                    "AlexaGBRank",
                                    "JobsTrendString",
                                    "FundingStage",
                                    "ActiveRetirementParticipants",
                                    "AlexaAURank",
                                    "SourceColumn",
                                    "JobsTrendString1",
                                    "JobsRecentJobs",
                                    "AlexaAUPageViews",
                                    "AlexaGBPageViews",
                                    "FundingFiscalYear",
                                    "BusinessEstimatedAnnualSales_k",
                                    "FraudOrDishonesty"
                                  ]
                                },
                                "UniqueValueCountDS": {
                                  "flaggedItemCount": 0,
                                  "dataRuleName": "UniqueValueCountDS",
                                  "tenant": {
                                    "Identifier": "LETest1468546401920.LETest1468546401920.Production",
                                    "DisplayName": "LETest1468546401920_LP3",
                                    "UIVersion": "3.0"
                                  },
                                  "modelId": "ms__40ab80d5-3c97-4394-a540-929e36cf2fd1-SelfServ",
                                  "flaggedColumnNames": []
                                }
                              },
                              "ruleNameToRowRuleResults": {
                                "HighlyPredictiveSmallPopulationDS": {
                                  "flaggedItemCount": 41,
                                  "dataRuleName": "HighlyPredictiveSmallPopulationDS",
                                  "tenant": {
                                    "Identifier": "LETest1468546401920.LETest1468546401920.Production",
                                    "DisplayName": "LETest1468546401920_LP3",
                                    "UIVersion": "3.0"
                                  },
                                  "modelId": "ms__40ab80d5-3c97-4394-a540-929e36cf2fd1-SelfServ",
                                  "flaggedRowIdAndColumnNames": {
                                    "6111987": [
                                      "SourceColumn"
                                    ],
                                    "24931170": [
                                      "SourceColumn"
                                    ],
                                    "40647887": [
                                      "Industry_Group"
                                    ],
                                    "43309438": [
                                      "Industry_Group"
                                    ],
                                    "43379366": [
                                      "Industry_Group"
                                    ],
                                    "43426596": [
                                      "Industry_Group"
                                    ],
                                    "44968290": [
                                      "Industry_Group"
                                    ],
                                    "45926670": [
                                      "SourceColumn"
                                    ],
                                    "47104034": [
                                      "SourceColumn"
                                    ],
                                    "48305462": [
                                      "SourceColumn"
                                    ],
                                    "49127833": [
                                      "SourceColumn"
                                    ],
                                    "50739888": [
                                      "SourceColumn"
                                    ],
                                    "51195637": [
                                      "Industry_Group"
                                    ],
                                    "51259362": [
                                      "SourceColumn"
                                    ],
                                    "51594981": [
                                      "SourceColumn"
                                    ],
                                    "52858677": [
                                      "SourceColumn"
                                    ],
                                    "53423107": [
                                      "SourceColumn"
                                    ],
                                    "53791723": [
                                      "SourceColumn"
                                    ],
                                    "53798719": [
                                      "SourceColumn"
                                    ],
                                    "55104437": [
                                      "Industry_Group"
                                    ],
                                    "55604370": [
                                      "Industry_Group",
                                      "Title_Level"
                                    ],
                                    "57062000": [
                                      "SourceColumn"
                                    ],
                                    "57109064": [
                                      "SourceColumn"
                                    ],
                                    "57678998": [
                                      "SourceColumn"
                                    ],
                                    "58529714": [
                                      "Title_Level"
                                    ],
                                    "58626464": [
                                      "Industry_Group"
                                    ],
                                    "59832676": [
                                      "SourceColumn"
                                    ],
                                    "59878171": [
                                      "SourceColumn"
                                    ],
                                    "59970768": [
                                      "SourceColumn"
                                    ],
                                    "60360107": [
                                      "SourceColumn"
                                    ],
                                    "60921689": [
                                      "SourceColumn"
                                    ],
                                    "60925464": [
                                      "SourceColumn"
                                    ],
                                    "61655595": [
                                      "SourceColumn"
                                    ],
                                    "61713724": [
                                      "Industry_Group"
                                    ],
                                    "62039822": [
                                      "SourceColumn"
                                    ],
                                    "62721524": [
                                      "SourceColumn"
                                    ],
                                    "62737580": [
                                      "SourceColumn"
                                    ],
                                    "62837373": [
                                      "Industry_Group"
                                    ],
                                    "62908209": [
                                      "Title_Level"
                                    ],
                                    "63617312": [
                                      "Industry_Group"
                                    ],
                                    "68294156": [
                                      "Title_Level"
                                    ]
                                  },
                                  "flaggedRowIdAndPositiveEvent": {
                                    "6111987": true,
                                    "24931170": true,
                                    "40647887": false,
                                    "43309438": false,
                                    "43379366": true,
                                    "43426596": false,
                                    "44968290": false,
                                    "45926670": true,
                                    "47104034": true,
                                    "48305462": true,
                                    "49127833": true,
                                    "50739888": true,
                                    "51195637": false,
                                    "51259362": true,
                                    "51594981": true,
                                    "52858677": true,
                                    "53423107": true,
                                    "53791723": false,
                                    "53798719": true,
                                    "55104437": true,
                                    "55604370": false,
                                    "57062000": true,
                                    "57109064": false,
                                    "57678998": false,
                                    "58529714": true,
                                    "58626464": false,
                                    "59832676": true,
                                    "59878171": true,
                                    "59970768": true,
                                    "60360107": true,
                                    "60921689": true,
                                    "60925464": true,
                                    "61655595": true,
                                    "61713724": false,
                                    "62039822": true,
                                    "62721524": true,
                                    "62737580": true,
                                    "62837373": false,
                                    "62908209": false,
                                    "63617312": false,
                                    "68294156": false
                                  },
                                  "numPositiveEvents": 26
                                }
                              }
                            }
                };
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultErrors: data.errorMsg
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };
});
