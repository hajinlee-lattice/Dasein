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
            // showAll: false,
            // showLatticeAttr: false,
            // showCustomAttr: true,
            totalWarnedColumnCount: 0,
            // latticeWarnedColumnCount: 0,
            // customWarnedColumnCount: 0,
            interface: {
                totalExcludedColumnCount: 0
            },
            columnWarningsToDisplay: []
        });

        vm.columnWarningsToDisplay = vm.allColumnWarnings;
        vm.allColumnWarnings.forEach(function(columnWarning) {
            vm.totalWarnedColumnCount += columnWarning.flaggedItemCount;
            ReviewData.dataRules.forEach(function(dataRule) {
                if (dataRule.enabled && dataRule.name == columnWarning.dataRuleName) {
                    vm.interface.totalExcludedColumnCount += dataRule.columnsToRemediate.length;
                }
            });
        });

        /*
        MetadataStore.GetMetadataForModel(modelId).then(function(modelMetadata) {
            vm.modelMetadata = modelMetadata;
            vm.allColumnWarnings.forEach(function(columnWarning) {
                vm.totalWarnedColumnCount += columnWarning.flaggedItemCount;
                ReviewData.dataRules.forEach(function(dataRule) {
                    if (dataRule.enabled) {
                        vm.interface += dataRule.flaggedItemCount;
                    }
                };
                    if (dataRule.enabled && isLatticeColumnWarning(columnWarning) && columnWarning.dataRuleName == dataRule.name) {
                        vm.latticeWarnedColumnCount += dataRule.columnsToRemediate.length;
                    } else if (dataRule.enabled && isNonLatticeColumnWarning(columnWarning) && columnWarning.dataRuleName == dataRule.name) {
                        vm.customWarnedColumnCount += dataRule.columnsToRemediate.length;
                    }
                });
                vm.displayNonLatticeWarningsClicked();
            });
        });
        */

        vm.displayLatticeWarningsClicked = function() {
            vm.showAll = false
            vm.showCustomAttr = false,
            vm.showLatticeAttr = true;
            vm.columnWarningsToDisplay = [];

            vm.allColumnWarnings.forEach(function(columnWarning) {
                if (isLatticeColumnWarning(columnWarning)) {
                    vm.columnWarningsToDisplay.push(columnWarning);
                }
            });
        };

        vm.displayNonLatticeWarningsClicked = function() {
            vm.showAll = false,
            vm.showLatticeAttr = false,
            vm.showCustomAttr = true;
            vm.columnWarningsToDisplay = [];

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
        };

        vm.isLatticeAttribute = function(column) {
            return MetadataService.IsLatticeAttribute(column);
        };

        vm.findMetadataItemByColumnName = function(columnName) {
            for (var i in vm.modelMetadata) {
                if (vm.modelMetadata[i]['ColumnName'].toLowerCase() == columnName.toLowerCase()) {
                    return vm.modelMetadata[i];
                }
            }
        };

        function isLatticeColumnWarning(columnWarning) {
            if (columnWarning.flaggedColumnNames != null) {
                for (var i in columnWarning.flaggedColumnNames) {
                    var flaggedColumn = vm.findMetadataItemByColumnName(columnWarning.flaggedColumnNames[i]);
                    if (flaggedColumn != null && vm.isLatticeAttribute(flaggedColumn)) {
                        return true;
                    }
                }
            }
            return false;
        }

        function isNonLatticeColumnWarning(columnWarning) {
            if (columnWarning.flaggedColumnNames != null) {
                for (var i in columnWarning.flaggedColumnNames) {
                    var flaggedColumn = vm.findMetadataItemByColumnName(columnWarning.flaggedColumnNames[i]);
                    if (flaggedColumn != null && !vm.isLatticeAttribute(flaggedColumn)) {
                        return true;
                    }
                }
            }
            return false;
        }
})
.directive('modelReviewColumnWarning', function() {
    return {
        restrict: 'EA',
        templateUrl: 'app/models/views/ColumnWarningRow.html',
        scope: {
            column: '=',
            rules: '=',
            data: '=',
            interface: '='
        },
        controller: ['$scope', '$stateParams', 'ModelService', 'ModelReviewStore', 'MetadataStore',
            'RefineModelThresholdModal', function($scope, $stateParams, ModelService, ModelReviewStore, MetadataStore, RefineModelThresholdModal) {

            $scope.modelId = $stateParams.modelId;
            $scope.columnWarning = $scope.column;
            $scope.ruleNameToDataRules = $scope.rules;
            $scope.columnWarningExpanded = false;
            $scope.dataRule = $scope.ruleNameToDataRules[$scope.columnWarning.dataRuleName];
            $scope.columnsToRemediate = $scope.dataRule.columnsToRemediate.slice(0);
            $scope.ReviewData = $scope.data;
            // $scope.latticeColumnNames = [];
            // $scope.customColumnNames = [];

            /**
            $scope.columnWarning.flaggedColumnNames.forEach(function(columnName) {
                var metadataItem = $scope.$parent.vm.findMetadataItemByColumnName(columnName);
                if ($scope.$parent.vm.isLatticeAttribute(metadataItem)) {
                    $scope.latticeColumnNames.push(columnName);
                } else {
                    $scope.customColumnNames.push(columnName);
                }
            });
            */

            $scope.warningExcludeToggled = function($event, warning) {
                if ($event == null) {
                    $event.preventDefault();
                }

                var ruleEnabledBefore = $scope.dataRule.enabled;
                if (ruleEnabledBefore && $scope.columnsToRemediate.length < $scope.columnWarning.flaggedItemCount) { // warning was partially enabled
                    $scope.interface.totalExcludedColumnCount -= $scope.columnsToRemediate.length;
                    $scope.columnsToRemediate = $scope.columnWarning.flaggedColumnNames.slice(0);
                    $('#' + warning.dataRuleName).trigger('click');
                } else if (ruleEnabledBefore) { // warning was all enabled
                    $scope.interface.totalExcludedColumnCount -= $scope.columnsToRemediate.length;
                    $scope.columnsToRemediate = [];
                    $scope.dataRule.enabled = false;
                } else {
                    $scope.columnsToRemediate = $scope.columnWarning.flaggedColumnNames.slice(0);
                    $scope.dataRule.enabled = true;
                }
                $scope.interface.totalExcludedColumnCount += $scope.columnsToRemediate.length;
                $scope.dataRule.columnsToRemediate = $scope.columnsToRemediate.slice(0);
                ModelReviewStore.AddDataRule($scope.modelId, $scope.dataRule);
            };

            $scope.columnExcludeToggled = function($event, columnName) {
                if ($scope.columnRemediated(columnName)) {
                    $scope.interface.totalExcludedColumnCount--;
                    $scope.columnsToRemediate.splice($scope.columnsToRemediate.indexOf(columnName), 1);
                    if ($scope.columnsToRemediate.length == 0) {
                        $scope.dataRule.enabled = false;
                    }
                } else {
                    $scope.interface.totalExcludedColumnCount++;
                    $scope.columnsToRemediate.push(columnName);
                    if ($scope.columnsToRemediate.length == 1) {
                        $scope.dataRule.enabled = true;
                    }
                }
                $scope.dataRule.columnsToRemediate = $scope.columnsToRemediate.slice(0);
                ModelReviewStore.AddDataRule($scope.modelId, $scope.dataRule);
            };

            $scope.columnRemediated = function(columnName) {
                return $scope.columnsToRemediate.indexOf(columnName) > -1;
            };

            $scope.expandColumnWarningClicked = function() {
                $scope.columnWarningExpanded = !$scope.columnWarningExpanded;
            };
        }]
    };
});
