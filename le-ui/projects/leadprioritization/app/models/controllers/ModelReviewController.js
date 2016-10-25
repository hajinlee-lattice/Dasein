angular.module('mainApp.models.review', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.models.services.ModelService',
    'mainApp.setup.services.MetadataService',
    'lp.models.review'
])
.controller('ModelReviewRowController', function($scope, $stateParams, _, $rootScope, ResourceUtility, ModelService, ModelReviewStore,
    ReviewData, Model) {
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

        $rootScope.$broadcast('RowWarningToggled', warning, vm.ruleNameToDataRules[warning.dataRuleName]);
    };

    vm.rowWarnings.forEach(function(rowWarning) {
        if (vm.ruleNameToDataRules[rowWarning.dataRuleName].enabled) {
            vm.rowsExcluded += rowWarning.flaggedItemCount;
        }
    });
})
.controller('ModelReviewColumnController', function($scope, _, $stateParams, ModelService, MetadataService, ModelReviewStore,
    ReviewData) {
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
            totalLatticeColumnCount: 0,
            totalCustomColumnCount: 0,
            interface: {
                totalExcludedColumnCount: 0,
                show: 'custom'
            },
            columnWarningsToDisplay: [],
            columnNameToIsLatticeAttr: {},
            columnNameToColumnDisplayName: {}
        });

        MetadataService.GetMetadataForModel(modelId).then(function(modelMetadata) {
            vm.modelMetadata = modelMetadata.ResultObj;
            vm.allColumnWarnings.forEach(function(columnWarning) {
                vm.totalWarnedColumnCount += columnWarning.flaggedItemCount;

                columnWarning.flaggedColumnNames.forEach(function(columnName) {
                    var column =  vm.findMetadataItemByColumnName(columnName);
                    vm.columnNameToIsLatticeAttr[columnName] = vm.isLatticeAttribute(column);
                    vm.columnNameToIsLatticeAttr[columnName] ? vm.totalLatticeColumnCount++ : vm.totalCustomColumnCount++;
                    vm.columnNameToColumnDisplayName[columnName] = column.DisplayName;
                });
                ReviewData.dataRules.forEach(function(dataRule) {
                    for (var i in dataRule.columnsToRemediate) {
                        if (dataRule.name in ReviewData.ruleNameToColumnRuleResults && ! dataRule.columnsToRemediate[i] in ReviewData.ruleNameToColumnRuleResults[dataRule.name].flaggedColumnNames) {
                            console.log('column: ' + dataRule.columnsToRemediate[i] + ' removed for rule: ' + dataRule.name);
                            dataRule.columnsToRemediate.splice(i, 1);
                        }
                    }
                    ModelReviewStore.AddDataRule(modelId, dataRule);
                });
                if (ruleNameToDataRules[columnWarning.dataRuleName].enabled) {
                    vm.interface.totalExcludedColumnCount += vm.ruleNameToDataRules[columnWarning.dataRuleName].columnsToRemediate.length;
                }
            });
            vm.displayNonLatticeWarningsClicked();
        });

        vm.displayLatticeWarningsClicked = function() {
            vm.showAll = false
            vm.showCustomAttr = false,
            vm.showLatticeAttr = true;
            vm.columnWarningsToDisplay = [];

            ModelReviewStore.GetReviewData(modelId).then(function(reviewData) {
                for (var i in reviewData.dataRules) {
                    ruleNameToDataRules[ReviewData.dataRules[i].name] = reviewData.dataRules[i];
                }
                vm.allColumnWarnings.forEach(function(columnWarning) {
                    if (isLatticeColumnWarning(columnWarning)) {
                        vm.columnWarningsToDisplay.push(columnWarning);
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

            ModelReviewStore.GetReviewData(modelId).then(function(reviewData) {
                for (var i in reviewData.dataRules) {
                    ruleNameToDataRules[ReviewData.dataRules[i].name] = reviewData.dataRules[i];
                }
                vm.allColumnWarnings.forEach(function(columnWarning) {
                    if (isNonLatticeColumnWarning(columnWarning)) {
                        vm.columnWarningsToDisplay.push(columnWarning);
                    }
                });
            });
            vm.interface.show = 'custom';
        };

        vm.displayAllWarningsClicked = function() {
            vm.showAll = true,
            vm.showLatticeAttr = false,
            vm.showCustomAttr = false;
            vm.columnWarningsToDisplay = [];

            ModelReviewStore.GetReviewData(modelId).then(function(reviewData) {
                for (var i in reviewData.dataRules) {
                    ruleNameToDataRules[ReviewData.dataRules[i].name] = reviewData.dataRules[i];
                }
                vm.allColumnWarnings.forEach(function(columnWarning) {
                    vm.columnWarningsToDisplay.push(columnWarning);
                });
            });
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
        controller: ['$scope', '$stateParams', 'ModelService', 'ModelReviewStore',
            function($scope, $stateParams, ModelService, ModelReviewStore) {

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
            $scope.columnNameToColumnDisplayName = $scope.$parent.vm.columnNameToColumnDisplayName;

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
                    $scope.interface.totalExcludedColumnCount += warning.flaggedColumnNames.length;
                    $scope.columnsToRemediate = $scope.columnWarning.flaggedColumnNames.slice(0);
                    $('#' + warning.dataRuleName).trigger('click');
                } else if (ruleEnabledBefore) { // warning was all enabled
                    $scope.interface.totalExcludedColumnCount -= $scope.columnsToRemediate.length;
                    $scope.columnsToRemediate = [];
                    $scope.dataRule.enabled = false;
                } else {
                    $scope.columnsToRemediate = $scope.columnWarning.flaggedColumnNames.slice(0);
                    $scope.interface.totalExcludedColumnCount += warning.flaggedColumnNames.length;
                    $scope.dataRule.enabled = true;
                }
                $scope.dataRule.columnsToRemediate = $scope.columnsToRemediate;
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

            $scope.shouldBeDisabled = function() {
                return $scope.columnWarning.flaggedItemCount == 0;
            };
        }]
    };
});
