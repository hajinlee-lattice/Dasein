export default function () {
    'ngInject';

    return {
        restrict: 'A',
        scope: {
            vm: '=',
            count: '=',
            enrichment: '=',
            cube: '='
        },
        controllerAs: 'vm',
        templateUrl: '/components/datacloud/explorer/attributetile/attributetile.component.html',
        controller: function (
            $scope, $state, $document, $timeout, $interval, Notice,
            QueryStore, DataCloudStore, NumberUtility, QueryTreeService, RatingsEngineStore
        ) {
            'ngInject';

            var vm = $scope.vm;
            angular.extend(vm, {
                enrichment: $scope.enrichment,
                cube: $scope.cube,
                bktlist: [],
            });

            vm.booleanStats = function (stats) {
                var booleans = {};

                for (var i in stats) {
                    var stat = stats[i];
                    booleans[stat.Lbl] = stat;
                }

                return booleans;
            }

            vm.getStatSort = function () {
                return vm.section == 're.model_iteration' ? '-Lift' : '-Cnt';
            }

            vm.goToEnumPicker = function (bucket, enrichment) {
                QueryTreeService.setPickerObject({
                    item: enrichment
                    //restriction: bucket 
                });

                var entity = enrichment.Entity;
                var fieldname = enrichment.ColumnId;
                var state = '';

                switch ($state.current.name) {
                    case 'home.ratingsengine.rulesprospects.segment.attributes.rules':
                        state = 'home.ratingsengine.rulesprospects.segment.attributes.rules.picker';
                        break;
                    case 'home.ratingsengine.dashboard.segment.attributes.add':
                        state = 'home.ratingsengine.dashboard.segment.attributes.rules.picker';
                        break;
                    default:
                        state = 'home.segment.explorer.enumpicker';
                }

                $state.go(state, {
                    entity: entity,
                    fieldname: fieldname
                });
            }

            function getBarChartConfig() {
                if ($scope.barChartConfig === undefined) {

                    $scope.barChartConfig = {
                        'data': {
                            'tosort': false,
                            'sortBy': 'Cnt',
                            'trim': true,
                            'top': 5,
                        },
                        'chart': {
                            'header': 'Attributes Value',
                            'emptymsg': '',
                            'usecolor': false,
                            'color': '#2E6099',
                            'mousehover': true,
                            'type': 'integer',
                            'showstatcount': true,
                            'maxVLines': 3,
                            'showVLines': false,
                        },
                        'vlines': {
                            'suffix': ''
                        },
                        'columns': [{
                            'field': 'Cnt',
                            'label': 'Records',
                            'type': 'number',
                            'chart': true,
                        }]
                    };
                }
                return $scope.barChartConfig;
            }

            function getBarChartLiftConfig() {
                if ($scope.barChartLiftConfig === undefined) {
                    $scope.barChartLiftConfig = {
                        'data': {
                            'tosort': false,
                            'sortBy': 'Cnt',
                            'trim': true,
                            'top': 5,
                        },
                        'chart': {
                            'header': 'Attributes Value',
                            'emptymsg': '',
                            'usecolor': false,
                            'color': '#2E6099',
                            'mousehover': true,
                            'type': 'decimal',
                            'showstatcount': true,
                            'maxVLines': 3,
                            'showVLines': true,
                        },
                        'vlines': {
                            'suffix': 'x'
                        },
                        'columns': [{
                            'field': 'Lift',
                            'label': 'Lifts',
                            'type': 'string',
                            'suffix': 'x',
                            'chart': true
                        },
                        {
                            'field': 'Cnt',
                            'label': 'Records',
                            'type': 'number',
                            'chart': false,
                        }
                        ]
                    };
                }
                return $scope.barChartLiftConfig;
            }

            vm.getChartConfig = function (list) {

                vm.bktlist = [];
                if (vm.cube) {
                    vm.bktlist = vm.getData(vm.enrichment.Entity, vm.enrichment.ColumnId);
                } else {
                    DataCloudStore.getCube().then(function (result) {
                        vm.cube = result;
                        vm.bktlist = vm.getData(vm.enrichment.Entity, vm.enrichment.ColumnId);
                    });
                }

                // console.log('Request chart config on list ', list);
                if (list != null && list.length > 0 && list[0].Lift != undefined) {
                    return getBarChartLiftConfig();
                }
                return getBarChartConfig();
            }

            vm.getData = function (entity, columnId) {
                var data = vm.cube.data[entity].Stats[columnId].Bkts.List;
                return data;
            }

            vm.chartRowClicked = function (stat, enrichment) {
                // console.log('Stat clicked: ',stat, ' - Enrichment: ', enrichment);
                vm.selectSegmentAttributeRange(enrichment, stat, (vm.section != 'segment.analysis'));
            }
            vm.NumberUtility = NumberUtility;


            vm.getOperationLabel = function (type, bucketRestriction) {
                return QueryTreeService.getOperationLabel(type, bucketRestriction);
            }

            vm.getOperationValue = function (bucketRestriction, type) {
                return QueryTreeService.getOperationValue(bucketRestriction, type);
            }

            vm.getQuerySnippet = function (enrichment, rule, type) {
                console.log('attrtile getQuerySnippet() enrichment', enrichment);
                console.log('attrtile getQuerySnippet() rule', rule);
                console.log('attrtile getQuerySnippet() type', type);
                console.log('attrtile getQuerySnippet() vm.cube', vm.cube);
                var querySnippet = enrichment.DisplayName + ' ';
                if (vm.cube && vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId] && vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts) { //bucketable attributes
                    querySnippet += 'is ';
                    querySnippet += (type == 'Enum' && rule.bucketRestriction.bkt.Vals && rule.bucketRestriction.bkt.Vals.length > 1) ? vm.generateBucketLabel(rule.bucketRestriction.bkt) : rule.bucketRestriction.bkt.Lbl;
                } else { //non-bucketable attributes e.g. pure-string
                    querySnippet += vm.getOperationLabel('String', rule.bucketRestriction) + ' ' + vm.getOperationValue(rule.bucketRestriction, 'String');
                }
                return querySnippet;
            }

            vm.showFreeTextAttributeCard = function (enrichment) {
                return vm.cube && vm.isBktEmpty(enrichment) && DataCloudStore.validFreeTextTypes.indexOf(enrichment.FundamentalType) >= 0 &&
                    (!vm.lookupMode && ['wizard.ratingsengine_segment', 'edit', 'team'].indexOf(vm.section) == -1)
            }

            vm.showInvalidAttributeCard = function (enrichment) {
                return vm.cube && vm.isBktEmpty(enrichment) && DataCloudStore.validFreeTextTypes.indexOf(enrichment.FundamentalType) == -1 &&
                    (!vm.lookupMode && ['wizard.ratingsengine_segment', 'edit', 'team'].indexOf(vm.section) == -1)
            }


            // Jamey's old version
            // vm.isBktEmpty = function (enrichment) {
            //     if (vm.cube && vm.cube.data && vm.cube.data[enrichment.Entity] && vm.cube.data[enrichment.Entity].Stats && vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId]) {
            //         return vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts == undefined ||
            //             !vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts.List.length;
            //     }
            // }
            vm.isBktEmpty = function (enrichment) {
                if (vm.cube && vm.cube.data && vm.cube.data[enrichment.Entity] && vm.cube.data[enrichment.Entity].Stats && vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId]) {
                    return vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts == undefined || vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts.List == undefined 
                        || !vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts.List.length;
                }
            }

            vm.NumberUtility = NumberUtility;

            vm.getBktListRating = function (enrichment) {
                return vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts.List;
            }

            vm.getWarning = function (warning) {
                let warnings = DataCloudStore.getWarnings();
                return warnings[warning];
            }

            vm.select = function (quantity) {
                let iterationFilter = DataCloudStore.getRatingIterationFilter();
                let categoryAttributes = vm.getAttributes([vm.category]);
                switch (quantity) {
                    case 'selectall':
                        
                        switch (iterationFilter) {
                            case 'all':
                                categoryAttributes.forEach( attr => attr.ApprovedUsage[0] = 'ModelAndAllInsights');
                            case 'used':
                                let usedCategoryAttributes = categoryAttributes.filter((item) => { return typeof item.ImportanceOrdering != 'undefined';});
                                usedCategoryAttributes.forEach( attr => attr.ApprovedUsage[0] = 'ModelAndAllInsights');
                            case 'warnings':
                                let warningsCategoryAttributes = categoryAttributes.filter((item) => { return item.HasWarnings;});
                                warningsCategoryAttributes.forEach( attr => attr.ApprovedUsage[0] = 'ModelAndAllInsights');
                            case 'disabled':
                                let disabledCategoryAttributes = categoryAttributes.filter((item) => { return item.ApprovedUsage[0] == 'None';});
                                disabledCategoryAttributes.forEach( attr => attr.ApprovedUsage[0] = 'ModelAndAllInsights');
                        };

                        var categories = vm.removeEmptyCategories(vm.categories);
                        vm.setCategory(categories[0]);
                        Notice.success({ message: 'Enabled all attributes for remodeling' });
                        break;
                    default:

                        switch (iterationFilter) {
                            case 'all':
                                categoryAttributes.forEach( attr => attr.ApprovedUsage[0] = 'None');
                            case 'used':
                                let usedCategoryAttributes = categoryAttributes.filter((item) => { return typeof item.ImportanceOrdering != 'undefined';});
                                usedCategoryAttributes.forEach( attr => attr.ApprovedUsage[0] = 'None');
                            case 'warnings':
                                let warningsCategoryAttributes = categoryAttributes.filter((item) => { return item.HasWarnings;});
                                warningsCategoryAttributes.forEach( attr => attr.ApprovedUsage[0] = 'None');
                            case 'disabled':
                                let disabledCategoryAttributes = categoryAttributes.filter((item) => { return item.ApprovedUsage[0] == 'None';});
                                disabledCategoryAttributes.forEach( attr => attr.ApprovedUsage[0] = 'None');
                        };

                        var categories = vm.removeEmptyCategories(vm.categories);
                        vm.setCategory(categories[0]);
                        Notice.warning({ message: 'Disabled all ' + vm.category + ' attributes from remodeling' });
                }
                RatingsEngineStore.setIterationEnrichments(vm.enrichments)
            }

            vm.toggleApprovedUsage = function (item) {
                switch (item.ApprovedUsage[0]) {
                    case 'None':
                        Notice.success({ message: 'Enabled attribute for remodeling' })
                        item.ApprovedUsage[0] = 'ModelAndAllInsights';
                        break;
                    default:
                        Notice.warning({ message: 'Disabled attribute from remodeling' })
                        item.ApprovedUsage[0] = 'None';
                }
                RatingsEngineStore.setIterationEnrichments(vm.enrichments)
            }

            vm.checkApprovedUsage = function (item) {
                return item.ApprovedUsage[0] != 'None';
            }

            vm.checkImportance = function (item) {
                return 'ImportanceOrdering' in item;
            }
        }
    };
};