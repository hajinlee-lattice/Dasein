angular.module('test.testData.ModelAlertsTestDataService', [
    'mainApp.appCommon.utilities.MetadataUtility'])
.service('ModelAlertsTestDataService', function() {

    this.GetSampleModelAlerts = function () {
        return {
            "ModelQualityWarnings": {
                "LowSuccessEvents": 437,
                "MinSuccessEvents": 500,
                "LowConversionPercentage": 0.8,
                "MinConversionPercentage": 1.0,
                "OutOfRangeRocScore": 0.62,
                "MinRocScore": 0.7,
                "MaxRocScore": 0.85,
                "ExcessiveDiscreteValuesAttributes": [{
                        "key": "attribuite1",
                        "value": "220"
                    }, {
                        "key": "attribuite2",
                        "value": "389"
                    }
                ],
                "MaxNumberOfDiscreteValues": 200,
                "ExcessivePredictiveAttributes": [{
                        "key": "attribuite1",
                        "value": "0.12"
                    }, {
                        "key": "attribuite2",
                        "value": "0.25"
                    }, {
                        "key": "attribuite2",
                        "value": "0.3"
                    }
                ],
                "MaxFeatureImportance": 0.1,
                "ExcessivePredictiveNullValuesAttributes": [{
                        "key": "attribuite1",
                        "value": "1.3"
                    }, {
                        "key": "attribuite2",
                        "value": "1.7"
                    }, {
                        "key": "attribuite2",
                        "value": "2.3"
                    }, {
                        "key": "attribuite2",
                        "value": "2.5"
                    }
                ],
                "MaxLiftForNull": 1.0
            },
            "MissingMetaDataWarnings": {
                "InvalidApprovedUsageAttributes": [
                    "attribuite1"
                ],
                "InvalidTagsAttributes": [
                    "attribuite1",
                    "attribuite2"
                ],
                "InvalidCategoryAttributes": [
                    "attribuite1",
                    "attribuite2",
                    "attribuite3"
                ],
                "InvalidDisplayNameAttributes": [
                    "attribuite1",
                    "attribuite2",
                    "attribuite3",
                    "attribuite4"
                ],
                "InvalidStatisticalTypeAttributes": [
                    "attribuite1",
                    "attribuite2",
                    "attribuite3",
                    "attribuite4",
                    "attribuite5",
                ]
            }
        };
    }; //end GetSampleModelAlerts()
})