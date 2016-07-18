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
            interface: {
                totalExcludedColumnCount: 0,
                latticeWarnedColumnCount: 0,
                customWarnedColumnCount : 0,
                show: 'custom'
            },
            columnWarningsToDisplay: [],
            columnNameToIsLatticeAttr: {}
        });

        /*
        vm.columnWarningsToDisplay = vm.allColumnWarnings;
        vm.allColumnWarnings.forEach(function(columnWarning) {
            vm.totalWarnedColumnCount += columnWarning.flaggedItemCount;
            ReviewData.dataRules.forEach(function(dataRule) {
                if (dataRule.enabled && dataRule.name == columnWarning.dataRuleName) {
                    vm.interface.totalExcludedColumnCount += dataRule.columnsToRemediate.length;
                }
            });
        });
        */

        MetadataStore.GetMetadataForModel(modelId).then(function(modelMetadata) {
            vm.modelMetadata = modelMetadata;
            vm.allColumnWarnings.forEach(function(columnWarning) {
                vm.totalWarnedColumnCount += columnWarning.flaggedItemCount;
                ReviewData.dataRules.forEach(function(dataRule) {
                    if (dataRule.name == columnWarning.dataRuleName) {
                        columnWarning.flaggedColumnNames.forEach(function(columnName) {
                            vm.columnNameToIsLatticeAttr[columnName] = vm.isLatticeAttribute(vm.findMetadataItemByColumnName(columnName));
                        });
                        if (dataRule.enabled) {
                            vm.interface.totalExcludedColumnCount += dataRule.flaggedItemCount;
                        }
                    }
                    /* if (dataRule.enabled && isLatticeColumnWarning(columnWarning) && columnWarning.dataRuleName == dataRule.name) {
                        vm.latticeWarnedColumnCount += dataRule.columnsToRemediate.length;
                    } else if (dataRule.enabled && isNonLatticeColumnWarning(columnWarning) && columnWarning.dataRuleName == dataRule.name) {
                        vm.customWarnedColumnCount += dataRule.columnsToRemediate.length;
                    } */
                });
            });
            vm.displayNonLatticeWarningsClicked();
        });

        vm.displayLatticeWarningsClicked = function() {
            vm.showAll = false
            vm.showCustomAttr = false,
            vm.showLatticeAttr = true;
            vm.columnWarningsToDisplay = [];
            vm.interface.latticeWarnedColumnCount = 0;

            ModelReviewStore.GetReviewData(modelId).then(function(reviewData) {
                for (var i in reviewData.dataRules) {
                    ruleNameToDataRules[ReviewData.dataRules[i].name] = reviewData.dataRules[i];
                }
                vm.allColumnWarnings.forEach(function(columnWarning) {
                    if (isLatticeColumnWarning(columnWarning)) {
                        vm.columnWarningsToDisplay.push(columnWarning);
                        columnWarning.flaggedColumnNames.forEach(function(columnName) {
                            if (vm.ruleNameToDataRules[columnWarning.dataRuleName].enabled && vm.columnNameToIsLatticeAttr[columnName]
                                && vm.ruleNameToDataRules[columnWarning.dataRuleName].columnsToRemediate.indexOf(columnName) > -1) {
                                vm.interface.latticeWarnedColumnCount++;
                            }
                        });
                    }
                });
            });
            vm.interface.show = 'lattice';
        };

        vm.displayNonLatticeWarningsClicked = function() {
            vm.showAll = false,
            vm.showLatticeAttr = false,
            vm.showCustomAttr = true;
            vm.columnWarningsToDisplay = [];
            vm.interface.customWarnedColumnCount = 0;

            ModelReviewStore.GetReviewData(modelId).then(function(reviewData) {
                for (var i in reviewData.dataRules) {
                    ruleNameToDataRules[ReviewData.dataRules[i].name] = reviewData.dataRules[i];
                }
                vm.allColumnWarnings.forEach(function(columnWarning) {
                    if (isNonLatticeColumnWarning(columnWarning)) {
                        vm.columnWarningsToDisplay.push(columnWarning);
                        columnWarning.flaggedColumnNames.forEach(function(columnName) {
                            if (vm.ruleNameToDataRules[columnWarning.dataRuleName].enabled && !vm.columnNameToIsLatticeAttr[columnName]
                                && vm.ruleNameToDataRules[columnWarning.dataRuleName].columnsToRemediate.indexOf(columnName) > -1) {
                                vm.interface.customWarnedColumnCount++;
                            }
                        });
                    }
                });
            });
            vm.interface.show = 'custom';
        };

        vm.displayAllWarningsClicked = function() {
            vm.showAll = true,
            vm.showLatticeAttr = false,
            vm.showCustomAttr = false;

            vm.columnWarningsToDisplay = vm.allColumnWarnings.slice(0);
            vm.interface.show = 'all';
        };

        vm.isLatticeAttribute = function(column) {
            if (column == null) { return false; }
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
            $scope.columnsToRemediate = $scope.dataRule.columnsToRemediate;
            $scope.ReviewData = $scope.data;
            $scope.columnTotal = $scope.columnWarning.flaggedItemCount;
            $scope.latticeWarningTotal = 0;
            $scope.customWarningTotal = 0;

            $scope.columnWarning.flaggedColumnNames.forEach(function(columnName) {
                if ($scope.$parent.vm.columnNameToIsLatticeAttr[columnName]) {
                    $scope.latticeWarningTotal++;
                } else if (!$scope.$parent.vm.columnNameToIsLatticeAttr[columnName]) {
                    $scope.customWarningTotal++;
                }
            });

            $scope.warningExcludeToggled = function($event, warning) {
                if ($event == null) {
                    $event.preventDefault();
                }

                var ruleEnabledBefore = $scope.dataRule.enabled;
                if (ruleEnabledBefore && $scope.columnsToRemediate.length < $scope.columnWarning.flaggedItemCount) { // warning was partially enabled
                    $scope.interface.totalExcludedColumnCount -= $scope.columnsToRemediate.length;
                    warning.flaggedColumnNames.forEach(function(columnName) {
                        if (!$scope.columnRemediated(columnName) && $scope.$parent.vm.columnNameToIsLatticeAttr[columnName]) {
                            $scope.interface.latticeWarnedColumnCount++;
                        } else if (!$scope.columnRemediated(columnName)) {
                            $scope.interface.customWarnedColumnCount++;
                        }
                    });
                    $scope.columnsToRemediate = $scope.columnWarning.flaggedColumnNames.slice(0);
                    $('#' + warning.dataRuleName).trigger('click');
                } else if (ruleEnabledBefore) { // warning was all enabled
                    $scope.interface.totalExcludedColumnCount -= $scope.columnsToRemediate.length;
                    warning.flaggedColumnNames.forEach(function(columnName) {
                        $scope.$parent.vm.columnNameToIsLatticeAttr[columnName] ? $scope.interface.latticeWarnedColumnCount-- : $scope.interface.customWarnedColumnCount--;
                    });
                    $scope.columnsToRemediate = [];
                    $scope.dataRule.enabled = false;
                } else {
                    $scope.columnsToRemediate = $scope.columnWarning.flaggedColumnNames.slice(0);
                    warning.flaggedColumnNames.forEach(function(columnName) {
                        $scope.$parent.vm.columnNameToIsLatticeAttr[columnName] ? $scope.interface.latticeWarnedColumnCount++ : $scope.interface.customWarnedColumnCount++;
                    });
                    $scope.dataRule.enabled = true;
                }
                $scope.interface.totalExcludedColumnCount += $scope.columnsToRemediate.length;
                $scope.dataRule.columnsToRemediate = $scope.columnsToRemediate;
                ModelReviewStore.AddDataRule($scope.modelId, $scope.dataRule);
            };

            $scope.columnExcludeToggled = function($event, columnName) {
                if ($scope.columnRemediated(columnName)) {
                    $scope.interface.totalExcludedColumnCount--;
                    $scope.columnsToRemediate.splice($scope.columnsToRemediate.indexOf(columnName), 1);
                    $scope.$parent.vm.columnNameToIsLatticeAttr[columnName] ? $scope.interface.latticeWarnedColumnCount-- : $scope.interface.customWarnedColumnCount--;
                    if ($scope.columnsToRemediate.length == 0) {
                        $scope.dataRule.enabled = false;
                    }
                } else {
                    $scope.interface.totalExcludedColumnCount++;
                    $scope.columnsToRemediate.push(columnName);
                    $scope.$parent.vm.columnNameToIsLatticeAttr[columnName] ? $scope.interface.latticeWarnedColumnCount++ : $scope.interface.customWarnedColumnCount++;
                    if ($scope.columnsToRemediate.length == 1) {
                        $scope.dataRule.enabled = true;
                    }
                }
                $scope.dataRule.columnsToRemediate = $scope.columnsToRemediate;
                ModelReviewStore.AddDataRule($scope.modelId, $scope.dataRule);
            };

            $scope.columnRemediated = function(columnName) {
                return $scope.columnsToRemediate.indexOf(columnName) > -1;
            };

            $scope.expandColumnWarningClicked = function() {
                $scope.columnWarningExpanded = !$scope.columnWarningExpanded;
            };

            $scope.shouldDisplayColumn = function(columnName) {
                return ($scope.$parent.vm.showCustomAttr && !$scope.$parent.vm.columnNameToIsLatticeAttr[columnName]) ||
                    ($scope.$parent.vm.showLatticeAttr && $scope.$parent.vm.columnNameToIsLatticeAttr[columnName]) ||
                    $scope.$parent.vm.showAll;
            };
        }]
    };
});
