angular
    .module('common.datacloud.explorer.attributetile', ['mainApp.appCommon.utilities.NumberUtility', 'common.datacloud.explorer.attributetile.bar.chart'])
    .directive('explorerAttributeTile', function () {
        return {
            restrict: 'A',
            scope: {
                vm: '=',
                count: '=',
                enrichment: '='
            },
            controllerAs: 'vm',
            templateUrl: '/components/datacloud/explorer/attributetile/attributetile.component.html',
            controller: function (
                $scope, $state, $document, $timeout, $interval,
                DataCloudStore, NumberUtility, QueryTreeService
            ) {
                var vm = $scope.vm;
                vm.enrichment = $scope.enrichment;
                angular.extend(vm, {});
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

                    $state.go('home.segment.explorer.enumpicker', {
                        entity: entity,
                        fieldname: fieldname
                    });
                }

                function getBarChartConfig() {
                    if ($scope.barChartConfig === undefined) {

                        $scope.barChartConfig = {
                            'data': {
                                'tosort': true,
                                'sortBy': '-Cnt',
                                'trim': true,
                                'top': 5,
                            },
                            'chart': {
                                'header':'Attributes Value',
                                'emptymsg': '',
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
                                'tosort': true,
                                'sortBy': 'Lbl',
                                'trim': true,
                                'top': 5,
                            },
                            'chart': {
                                'header':'Attributes Value',
                                'emptymsg': '',
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
                    if (list != null && list.length > 0 && list[0].Lift != undefined) {
                        return getBarChartLiftConfig();
                    }
                    return getBarChartConfig();
                }

                vm.getData = function (entity, columnId) {
                    var data = vm.cube.data[entity].Stats[columnId].Bkts.List;
                    // console.log('Data ',data);
                    return data;
                }

                vm.chartRowClicked = function (stat) {
                    console.log('Stat clicked --> ',stat);
                    vm.selectSegmentAttributeRange(vm.enrichment, stat, (vm.section != 'segment.analysis'));
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
                    if (vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts) { //bucketable attributes
                        querySnippet += 'is ';
                        querySnippet += (type == 'Enum' && rule.bucketRestriction.bkt.Vals.length > 1) ? vm.generateBucketLabel(rule.bucketRestriction.bkt) : rule.bucketRestriction.bkt.Lbl;
                    } else { //non-bucketable attributes e.g. pure-string
                        querySnippet += vm.getOperationLabel('String', rule.bucketRestriction) + ' ' + vm.getOperationValue(rule.bucketRestriction, 'String');
                    }
                    return querySnippet;
                }

                vm.NumberUtility = NumberUtility;
            }
        };
    });