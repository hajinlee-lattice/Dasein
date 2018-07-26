angular
    .module('common.datacloud.explorer.attributetile', ['mainApp.appCommon.utilities.NumberUtility', 'common.datacloud.explorer.attributetile.bar.chart'])
    .directive('explorerAttributeTile', function () {
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
                $scope, $state, $document, $timeout, $interval, 
                QueryStore, DataCloudStore, NumberUtility, QueryTreeService
            ) {
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
                                'header':'Attributes Value',
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
                                'header':'Attributes Value',
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
                    if(vm.cube) {
                        vm.bktlist = vm.getData(vm.enrichment.Entity, vm.enrichment.ColumnId);
                    } else {
                        DataCloudStore.getCube().then(function(result) {
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
                    var querySnippet = enrichment.DisplayName + ' ';
                    if (vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId] && vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts) { //bucketable attributes
                        querySnippet += 'is ';
                        querySnippet += (type == 'Enum' && rule.bucketRestriction.bkt.Vals && rule.bucketRestriction.bkt.Vals.length > 1) ? vm.generateBucketLabel(rule.bucketRestriction.bkt) : rule.bucketRestriction.bkt.Lbl;
                    } else { //non-bucketable attributes e.g. pure-string
                        querySnippet += vm.getOperationLabel('String', rule.bucketRestriction) + ' ' + vm.getOperationValue(rule.bucketRestriction, 'String');
                    }
                    return querySnippet;
                }

                vm.showFreeTextAttributeCard = function(enrichment) {
                    return vm.cube && vm.isBktEmpty(enrichment) &&  DataCloudStore.validFreeTextTypes.indexOf(enrichment.FundamentalType) >= 0 &&
                          (!vm.lookupMode && ['wizard.ratingsengine_segment','edit','team'].indexOf(vm.section) == -1)
                }

                vm.showInvalidAttributeCard = function(enrichment) {
                    return vm.cube && vm.isBktEmpty(enrichment) && DataCloudStore.validFreeTextTypes.indexOf(enrichment.FundamentalType) == -1 &&
                          (!vm.lookupMode && ['wizard.ratingsengine_segment','edit','team'].indexOf(vm.section) == -1)
                }

                vm.isBktEmpty = function(enrichment) {
                    return vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts == undefined || 
                            !vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts.List.length;
                }

                vm.NumberUtility = NumberUtility;
            }
        };
    });