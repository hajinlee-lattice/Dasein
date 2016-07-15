angular.module('mainApp.models.review', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.models.services.ModelService',
    'mainApp.setup.services.MetadataService',
    'mainApp.models.modals.RefineModelThresholdModal',
    'lp.models.review'
])
.controller('ModelReviewRowController', function($scope, $stateParams, _, $rootScope, ResourceUtility, ModelService, ModelReviewStore,
    ReviewData, RefineModelThresholdModal, Model) {
    var vm = this,
        ruleNameToDataRules = {},
        modelId = $stateParams.modelId;

    for (var i in ReviewData.dataRules) {
        ruleNameToDataRules[ReviewData.dataRules[i].name] = ReviewData.dataRules[i];
    }

    angular.extend(vm, {
        ruleNameToDataRules: ruleNameToDataRules,
        rowWarnings : _.values(ReviewData.ruleNameToRowRuleResults),
        totalRecords: Model.ModelDetails.TotalLeads,
        rowsExcluded: 0
    });

    var storedDataRules = ModelReviewStore.GetDataRules(modelId);
    if (storedDataRules != null) {
        storedDataRules.forEach(function(storeDataRule) {
            vm.ruleNameToDataRules[storeDataRule.name] = storeDataRule;
        })
    }

    vm.warningExcludeToggled = function($event, warning) {
        if ($event == null) {
            $event.preventDefault();
        }

        var beforeState = vm.ruleNameToDataRules[warning.dataRuleName].enabled;
        vm.ruleNameToDataRules[warning.dataRuleName].enabled = !beforeState;
        ModelReviewStore.SetReviewData(modelId, ReviewData);
        ModelReviewStore.AddDataRule(modelId, vm.ruleNameToDataRules[warning.dataRuleName]);
        if (beforeState == true) {
            vm.rowsExcluded -= warning.flaggedItemCount;
        } else {
            vm.rowsExcluded += warning.flaggedItemCount;
        }

        RefineModelThresholdModal.show();

        $rootScope.$broadcast('RowWarningToggled', warning, vm.ruleNameToDataRules[warning.dataRuleName]);
    };

    vm.rowWarnings.forEach(function(rowWarning) {
        if (vm.ruleNameToDataRules[rowWarning.dataRuleName].enabled) {
            vm.rowsExcluded += rowWarning.flaggedItemCount;
        }
    });
})
.controller('ModelReviewColumnController', function($scope, _, $stateParams, ModelService, MetadataService, ModelReviewStore,
    ReviewData, MetadataStore, RefineModelThresholdModal) {
        var vm = this,
            ruleNameToDataRules = {},
            modelId = $stateParams.modelId;

        for (var i in ReviewData.dataRules) {
            ruleNameToDataRules[ReviewData.dataRules[i].name] = ReviewData.dataRules[i];
        }

        angular.extend(vm, {
            ruleNameToDataRules: ruleNameToDataRules,
            allColumnWarnings : _.values(ReviewData.ruleNameToColumnRuleResults),
            showAll: false,
            showLatticeAttr: false,
            showCustomAttr: true,
            totalWarnedColumnCount: 0,
            latticeWarnedColumnCount: 0,
            customWarnedColumnCount: 0,
            columnWarningsToDisplay: []
        });
        var storedDataRules = ModelReviewStore.GetDataRules(modelId);
        if (storedDataRules != null) {
            storedDataRules.forEach(function(storeDataRule) {
                vm.ruleNameToDataRules[storeDataRule.name] = storeDataRule;
            });
        }

        vm.allColumnWarnings.forEach(function(columnWarning) {
            vm.totalWarnedColumnCount += columnWarning.flaggedItemCount;
            if (ruleNameToDataRules[columnWarning.dataRuleName].enabled) {
                if (isLatticeColumnWarning(columnWarning)) {
                    vm.latticeWarnedColumnCount += columnWarning.flaggedItemCount;
                } else if (isNonLatticeColumnWarning(columnWarning)) {
                    vm.customWarnedColumnCount += columnWarning.flaggedItemCount;
                }
            }
        });

        vm.displayLatticeWarningsClicked = function() {
            vm.columnWarningsToDisplay = [],
            vm.showAll = false,
            vm.showCustomAttr = false,
            vm.showLatticeAttr = true;
            vm.latticeWarnedColumnCount = 0;

            vm.allColumnWarnings.forEach(function(columnWarning) {
                if (isLatticeColumnWarning(columnWarning)) {
                    vm.columnWarningsToDisplay.push(columnWarning);
                }
            });
        };

        vm.displayNonLatticeWarningsClicked = function() {
            vm.columnWarningsToDisplay = [],
            vm.showAll = false,
            vm.showLatticeAttr = false,
            vm.showCustomAttr = true;
            vm.customWarnedColumnCount = 0;

            vm.allColumnWarnings.forEach(function(columnWarning) {
                if (isNonLatticeColumnWarning(columnWarning)) {
                    vm.columnWarningsToDisplay.push(columnWarning);
                }
            });
        };

        vm.displayAllWarningsClicked = function() {
            vm.showAll = true,
            vm.showLatticeAttr = false,
            vm.showCustomAttr = false;

            vm.columnWarningsToDisplay = vm.allColumnWarnings.slice(0);
            vm.customWarnedColumnCount = vm.totalWarnedColumnCount;
        };
        MetadataStore.GetMetadataForModel(modelId).then(function(modelMetadata) {
            vm.modelMetadata = modelMetadata;
            vm.displayNonLatticeWarningsClicked();
        });

        function isLatticeColumnWarning(columnWarning) {
            if (columnWarning.flaggedColumnNames != null) {
                for (var i in columnWarning.flaggedColumnNames) {
                    var flaggedColumn = findMetadataItemByColumnName(columnWarning.flaggedColumnNames[i]);
                    if (flaggedColumn != null && isLatticeAttribute(flaggedColumn)) {
                        return true;
                    }
                }
            }
            return false;
        }

        function isNonLatticeColumnWarning(columnWarning) {
            if (columnWarning.flaggedColumnNames != null) {
                for (var i in columnWarning.flaggedColumnNames) {
                    var flaggedColumn = findMetadataItemByColumnName(columnWarning.flaggedColumnNames[i]);
                    if (flaggedColumn != null && !isLatticeAttribute(flaggedColumn)) {
                        return true;
                    }
                }
            }
            return false;
        }

        function isLatticeAttribute(column) {
            return MetadataService.IsLatticeAttribute(column);
        }

        function findMetadataItemByColumnName(columnName) {
            for (var i in vm.modelMetadata) {
                if (vm.modelMetadata[i]['ColumnName'].toLowerCase() == columnName.toLowerCase()) {
                    return vm.modelMetadata[i];
                }
            }
        }
})
.directive('modelReviewColumnWarning', function() {
    return {
        restrict: 'EA',
        templateUrl: 'app/models/views/ColumnWarningRow.html',
        scope: {
            column: '=',
            rules: '=',
            data: '='
        },
        controller: ['$scope', '$stateParams', 'ModelService', 'ModelReviewStore', 'MetadataStore',
            'RefineModelThresholdModal', function($scope, $stateParams, ModelService, ModelReviewStore, MetadataStore, RefineModelThresholdModal) {

            $scope.modelId = $stateParams.modelId;
            $scope.columnWarning = $scope.column;
            $scope.ruleNameToDataRules = $scope.rules;
            $scope.columnWarningExpanded = false;
            $scope.dataRule = $scope.ruleNameToDataRules[$scope.columnWarning.dataRuleName];
            $scope.columnsToRemediate = $scope.dataRule.columnsToRemediate;
            $scope.ReviewData = $scope.data;

            $scope.warningExcludeToggled = function($event, warning) {
                if ($event == null) {
                    $event.preventDefault();
                }

                var ruleEnabledBefore = $scope.dataRule.enabled;

                if (ruleEnabledBefore && $scope.columnsToRemediate.length > 0 &&
                    $scope.columnsToRemediate.length < $scope.columnWarning.flaggedItemCount) {
                    $scope.columnsToRemediate = [];
                    $scope.dataRule.columnsToRemediate = [];
                } else {
                    $scope.columnsToRemediate = [];
                    $scope.dataRule.columnsToRemediate = [];
                    $scope.dataRule.enabled = !ruleEnabledBefore;
                }
                ModelReviewStore.AddDataRule($scope.modelId, $scope.dataRule);

                RefineModelThresholdModal.show();
            };

            $scope.columnExcludeToggled = function($event, columnName) {
                var ruleStateEnabled = $scope.dataRule.enabled;

                if (ruleStateEnabled && columnName in $scope.columnsToRemediate) {
                    $scope.columnsToRemediate.splice($scope.columnsToRemediate.indexOf(columnName), 1);
                    if ($scope.columnsToRemediate.length == 0) {
                        $scope.dataRule.enabled = false;
                        $scope.dataRule.columnsToRemediate = [];
                    }
                } else if (ruleStateEnabled) {
                    if ($scope.columnsToRemediate.length == 0) {
                        $scope.columnWarning.flaggedColumnNames.forEach(function(flaggedColumnName) {
                            if (flaggedColumnName != columnName) {
                                $scope.columnsToRemediate.push(flaggedColumnName);
                            }
                        });
                        if ($scope.columnsToRemediate.length == 0) {
                            $scope.dataRule.enabled = false;
                        }
                    } else {
                        $scope.columnsToRemediate.push(columnName);
                        if ($scope.columnsToRemediate.length == $scope.columnWarning.flaggedItemCount) {
                            $scope.columnsToRemediate = [];
                        }
                    }
                    $scope.dataRule.columnsToRemediate = $scope.columnsToRemediate;
                } else if (!ruleStateEnabled) {
                    $scope.dataRule.enabled = true;
                    $scope.columnsToRemediate.push(columnName);
                    if ($scope.columnsToRemediate.length == $scope.columnWarning.flaggedItemCount) {
                        $scope.columnsToRemediate = [];
                    }
                    $scope.dataRule.columnsToRemediate = $scope.columnsToRemediate;
                }

                ModelReviewStore.AddDataRule($scope.modelId, $scope.dataRule);
            };

            $scope.columnRemediated = function(columnName) {
                return columnName in $scope.columnsToRemediate;
            };

            $scope.expandColumnWarningClicked = function() {
                $scope.columnWarningExpanded = !$scope.columnWarningExpanded;
            };
        }]
    };
});
