angular.module('mainApp.models.review', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.models.services.ModelService',
    'mainApp.setup.services.MetadataService',
    'mainApp.models.modals.RefineModelThresholdModal',
    'lp.models.review'
])
.controller('ModelReviewRowController', function($scope, _, $rootScope, ResourceUtility, ModelService, ModelReviewStore,
    ReviewData, RefineModelThresholdModal, Model) {
    var vm = this,
        ruleNameToDataRules = {};

    for (var i in ReviewData.dataRules) {
        ruleNameToDataRules[ReviewData.dataRules[i].name] = ReviewData.dataRules[i];
    }

    angular.extend(vm, {
        ruleNameToDataRules: ruleNameToDataRules,
        rowWarnings : _.values(ReviewData.ruleNameToRowRuleResults),
        totalRecords: Model.ModelDetails.TotalLeads,
        rowsExcluded: 0
    });

    var storedDataRules = ModelReviewStore.GetDataRules();
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
        ModelReviewStore.SetReviewData(vm.modelId, ReviewData);
        ModelReviewStore.AddDataRule(vm.ruleNameToDataRules[warning.dataRuleName]);
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
        var storedDataRules = ModelReviewStore.GetDataRules();
        if (storedDataRules != null) {
            storedDataRules.forEach(function(storeDataRule) {
                vm.ruleNameToDataRules[storeDataRule.name] = storeDataRule;
            })
        }

        vm.warningExcludeToggled = function($event, warning) {
            if ($event == null) {
                $event.preventDefault();
            }

            var beforeState = ruleNameToDataRules[warning.dataRuleName].enabled;
            ruleNameToDataRules[warning.dataRuleName].enabled = !beforeState;
            ModelReviewStore.SetReviewData(vm.modelId, ReviewData);
            ModelReviewStore.AddDataRule(ruleNameToDataRules[warning.dataRuleName]);
            if (beforeState == true) {
                vm.totalWarnedColumnCount -= warning.flaggedItemCount;
                if (isLatticeColumnWarning(warning)) {
                    vm.latticeWarnedColumnCount -= warning.flaggedItemCount;
                } else {
                    vm.customWarnedColumnCount -= warning.flaggedItemCount;
                }
            } else {
                vm.totalWarnedColumnCount += warning.flaggedItemCount;
                if (isLatticeColumnWarning(warning)) {
                    vm.latticeWarnedColumnCount += warning.flaggedItemCount;
                } else {
                    vm.customWarnedColumnCount += warning.flaggedItemCount;
                }
            }
            if (vm.showAll) {
                vm.customWarnedColumnCount = vm.totalWarnedColumnCount;
            }

            RefineModelThresholdModal.show();

        };

        vm.allColumnWarnings.forEach(function(columnWarning) {
            if (ruleNameToDataRules[columnWarning.dataRuleName].enabled) {
                vm.totalWarnedColumnCount += columnWarning.flaggedItemCount;
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
                    if (ruleNameToDataRules[columnWarning.dataRuleName].enabled) {
                        vm.latticeWarnedColumnCount += columnWarning.flaggedItemCount;
                    }
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
                    if (ruleNameToDataRules[columnWarning.dataRuleName].enabled) {
                        vm.customWarnedColumnCount += columnWarning.flaggedItemCount;
                    }
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
            rules: '='
        },
        controller: ['$scope', '$stateParams', 'ModelService', 'MetadataService', 'ModelReviewStore', 'MetadataStore',
            'RefineModelThresholdModal', function($scope, $stateParams, ModelService, ModelReviewStore, MetadataStore, RefineModelThresholdModal) {

            $scope.modelId = $stateParams.modelId;
            $scope.columnWarning = $scope.column;
            $scope.ruleNameToDataRules = $scope.rules;
            $scope.columnWarningExpanded = false;

            $scope.warningExcludeToggled = function($event, warning) {
                if ($event == null) {
                    $event.preventDefault();
                }

                var beforeState = ruleNameToDataRules[warning.dataRuleName].enabled;
                ruleNameToDataRules[warning.dataRuleName].enabled = !beforeState;
                ModelReviewStore.SetReviewData($scope.modelId, ReviewData);
                ModelReviewStore.AddDataRule(ruleNameToDataRules[warning.dataRuleName]);
                if (beforeState == true) {
                    vm.totalWarnedColumnCount -= warning.flaggedItemCount;
                    if (isLatticeColumnWarning(warning)) {
                        vm.latticeWarnedColumnCount -= warning.flaggedItemCount;
                    } else {
                        vm.customWarnedColumnCount -= warning.flaggedItemCount;
                    }
                } else {
                    vm.totalWarnedColumnCount += warning.flaggedItemCount;
                    if (isLatticeColumnWarning(warning)) {
                        vm.latticeWarnedColumnCount += warning.flaggedItemCount;
                    } else {
                        vm.customWarnedColumnCount += warning.flaggedItemCount;
                    }
                }
                if (vm.showAll) {
                    vm.customWarnedColumnCount = vm.totalWarnedColumnCount;
                }

                RefineModelThresholdModal.show();
            };

            $scope.expandColumnWarningClicked = function() {
                $scope.columnWarningExpanded = !$scope.columnWarningExpanded;
            };
        }]
    };
});
