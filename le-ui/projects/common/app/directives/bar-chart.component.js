angular
    .module('common.directives.tilebarchart', [])
    .directive('barChart', function () {
        return {
            restrict: 'E',
            scope: {
                vm: '=?',
                bktlist: '=',
                callback: '&?',
                config: '=',
                statcount: '=?',
                enrichment: '=?'

            },
            templateUrl: '/components/charts/bar-chart.component.html',
            controller: function ($scope, $filter, $timeout) {

                /**
                 * Return the number with decimals truncated to the maxDigits.
                 * If not provided the maxDigits is 1
                 * @param {*} value 
                 * @param {number} maxDigits
                 */
                function getTruncatedValue(value, maxDigits) {
                    var numb = Number(value);
                    var max = maxDigits ? maxDigits : 1;
                    var ret = parseFloat(numb.toFixed(max));
                    return ret;
                }

                function getHighestStat(stats, fieldName) {
                    var highest = 0;
                    if (stats) {
                        stats.forEach(function (stat) {
                            if (stat[fieldName] > highest) {
                                highest = Number(stat[fieldName]);
                            }
                        });
                    }
                    return highest;
                }
                function getHighestRounded() {
                    var decimal = getRoundedNumber(($scope.highest % 1), 1);
                    var ret = $scope.highest;
                    var add = 0;
                    if (decimal > 0 && decimal > 0.5) {
                        add = 1 - decimal;
                    } else if (decimal > 0 && decimal < 0.5) {
                        add = 0.5 - decimal;
                    }
                    ret = $scope.highest + add;
                    ret = getRoundedNumber(ret, 1);
                    return ret;
                }

                function getHorizontalPercentage(stat, field, limit) {
                    var number = stat[field];

                    if (number && $scope.highest) {
                        var percentage = ((number / $scope.highest) * 100);

                        if (typeof limit != 'undefined') {
                            percentage = percentage.toFixed(limit);
                        }
                        return Number(percentage) + '%';
                    }
                    return 0;
                }

                function getHorizontalPercentageSubDec(stat, field, limit) {
                    // var max = Math.ceil($scope.highest);
                    var max = Number(Math.round($scope.highest * 2) / 2);
                    if (max < $scope.highest) {
                        max = Number(Math.round(max));
                    }
                    var val = stat[field];
                    if (max && val) {
                        val = Number(val);
                        var percentage = (val * 100) / max;
                        return Number(percentage) + '%';
                    }
                    return 0;
                }

                /**
                 * Sort the data by sortBy value in the config object
                 */
                function sortData() {
                    if ($scope.bktlist == undefined) {
                        $scope.bktlist = [];
                    }
                    var field = $scope.sortBy;
                    if (field.startsWith('-')) {
                        field = field.substring(1, field.length);
                    }
                    $scope.bktlist.sort(function (item1, item2) {
                        var sortBy = field;
                        if (item1[sortBy] < item2[sortBy])
                            return -1;
                        if (item1[sortBy] > item2[sortBy])
                            return 1;
                        return 0;
                    });
                    if ($scope.sortBy.startsWith('-')) {
                        $scope.bktlist.reverse();
                    }
                }

                function onlyTopN() {
                    $scope.bktlist = $filter('limitTo')($scope.bktlist, $scope.top);
                }

                /**
                 * Return the column to use to draw the chart
                 */
                function getColumnForGraph() {
                    for (var i = 0; i < $scope.columns.length; i++) {
                        if ($scope.columns[i].chart != undefined && $scope.columns[i].chart === true) {
                            return $scope.columns[i];
                        }
                    }
                    return null;
                }

                function validateConfig() {
                    if ($scope.config == undefined) {
                        $scope.config = {
                            'data': {},
                            'chart': {},
                            'vlines': {},
                            'columns': {}
                        };
                    }
                    if (!$scope.config.data) {
                        $scope.config.data = {};
                    }
                    if (!$scope.config.chart) {
                        $scope.config.chart = {};
                    }
                    if (!$scope.config.vlines) {
                        $scope.config.vlines = {};
                    }
                    if (!$scope.config.columns) {
                        $scope.config.columns = {};
                    }
                }

                function getRoundedNumber(value, decimal) {
                    var rounded = Number(Math.round(value + 'e' + decimal) + 'e-' + decimal);
                    return rounded;
                }

                function validateData() {
                    if ($scope.bktlist == undefined) {
                        $scope.bktlist = [];
                    }
                    $scope.bktlist.forEach(function (element) {
                        if (element.lift) {
                            element.lift = getRoundedNumber(element.lift, $scope.decimal);
                        }
                    });

                }

                /**
                 * configuration:
                 * top: max number of rows
                 * bktlist: bucket list which containes data 
                 * color: color for the rows
                 * showfield: name field to show
                 */

                $scope.$watch('bktlist', function () {
                    if ($scope.bktlist.length > 0) {
                        $scope.init();
                    }
                });

                $scope.init = function () {

                    /************************************* Config ************************************************/
                    validateConfig();

                    /************************Data config ***********************/
                    $scope.tosort = $scope.config.data.tosort == undefined ? false : $scope.config.data.tosort;
                    $scope.sortBy = $scope.config.data.sortBy !== undefined ? $scope.config.data.sortBy : '-Cnt';
                    $scope.trimData = $scope.config.data.trim !== undefined ? $scope.config.data.trim : false;
                    $scope.top = $scope.config.data.top !== undefined ? $scope.config.data.top : 6;
                    $scope.decimal = $scope.config.data.decimal !== undefined ? $scope.config.data.decimal : 1;
                    /***********************************************************/

                    /************************** Chart Config ***********************/
                    $scope.header = $scope.config.chart.header !== undefined ? $scope.config.chart.header : 'Header';
                    $scope.emptymsg = $scope.config.chart.emptymsg !== undefined ? $scope.config.chart.emptymsg || 'No scored accounts available.' : 'No scored accounts available.';
                    $scope.color = $scope.config.chart.color !== undefined ? $scope.config.chart.color : '#D0D1D0';
                    $scope.usecolor = $scope.config.chart.usecolor !== undefined ? Boolean($scope.config.chart.usecolor) : true;
                    $scope.mousehover = $scope.config.chart.mousehover !== undefined ? $scope.config.chart.mousehover : false;
                    $scope.hovercolor = $scope.config.chart.hovercolor !== undefined ? $scope.config.chart.hovercolor : $scope.color;
                    $scope.chartType = $scope.config.chart.type !== undefined ? $scope.config.chart.type : 'decimal';
                    $scope.showVLines = $scope.config.chart.showVLines !== undefined ? Boolean($scope.config.chart.showVLines) : false;
                    $scope.maxVLines = $scope.config.chart.maxVLines !== undefined ? $scope.config.chart.maxVLines : 3;
                    $scope.showstatcount = $scope.config.chart.showstatcount !== undefined ? $scope.config.chart.showstatcount : false;

                    /***************************************************************/
                    /**************************** Columns Config ***********************************/
                    $scope.columns = $scope.config.columns ? $scope.config.columns : [];

                    /*********************************************************************************************/

                    /****************************** V Lines Config ******************************************/
                    $scope.vlinesSuffix = $scope.config.vlines.suffix !== undefined ? $scope.config.vlines.suffix : '';

                    /****************************************************************************************/

                    $scope.bktlist = $scope.bktlist !== undefined ? $scope.bktlist : [];
                    if ($scope.tosort) {
                        sortData();
                    }
                    if ($scope.trimData && $scope.bktlist.length > $scope.top) {
                        onlyTopN();
                    }
                    //*****************************************/

                    // console.log($scope.bktlist);
                    validateData();

                    $scope.highest = 0;
                    var column = getColumnForGraph();
                    if (column !== null) {
                        $scope.highest = getHighestStat($scope.bktlist, column.field);
                        $scope.highest = getHighestRounded();
                    }
                    // console.log('Highest ', $scope.highest);
                }

                $scope.init();

                /**
                 * Return the columns after the chart
                 * each column can have the following config
                 *  field: 'Lift',
                 *  label: 'Lifts',
                 *  type: 'string',
                 *  suffix: 'x',
                 *  chart: true
                 */
                $scope.getColumns = function () {
                    return $scope.columns;
                }

                /**
                 * Return the value of the specific cell based on the type of the column
                 * If the type is 'string' the suffix is appended
                 * @param {*} stat 
                 * @param {*} column 
                 */
                $scope.getValue = function (stat, column) {
                    var numb = 0;
                    var ret = 0;
                    switch (column.type) {
                        case 'number':
                            {
                                return getTruncatedValue(stat[column.field]).toLocaleString();
                            }
                        case 'string':
                            {
                                return getTruncatedValue(stat[column.field]) + column.suffix;
                            }
                        default:
                            {
                                return getTruncatedValue(stat[column.field]);
                            }
                    }
                }

                /**
                 * Return the value showVLines fron the config object. 
                 * If not set return false
                 */
                $scope.showVerticalLines = function () {
                    return $scope.showVLines;
                }

                $scope.getBarColor = function (stat) {
                    if ($scope.usecolor == true || $scope.getStatCount(stat) > 0) {
                        return $scope.color;
                    } else {
                        return '#939393';
                    }
                }

                $scope.getMouseOverColor = function () {
                    if ($scope.mousehover) {
                        return $scope.hovercolor;
                    } else {
                        return $scope.color;;
                    }
                }



                $scope.getHorizontalPercentage = function (stat, limit) {
                    var column = getColumnForGraph();
                    if (column == null) {
                        return 0;
                    }
                    switch ($scope.chartType) {
                        case 'decimal':
                            {
                                return getHorizontalPercentageSubDec(stat, column.field, limit);
                            }
                        default:
                            {
                                return getHorizontalPercentage(stat, column.field, limit);
                            }
                    }
                };

                $scope.getVerticalLines = function () {
                    if (!$scope.bktlist || $scope.bktlist.length == 0) {
                        return [];
                    }
                    if ($scope.vertcalLines === undefined) {
                        var lines = [];
                        var max = $scope.highest;//getMaxRounded();
                        if (max > 0) {
                            if ($scope.bktlist.length == 1) {
                                lines.push({
                                    'perc': Number(100 / 2) + '%',
                                    'label': (max / 2) + $scope.vlinesSuffix
                                });
                                lines.push({
                                    'perc': 100 + '%',
                                    'label': max + $scope.vlinesSuffix
                                });
                            } else {
                                var intervallRange = Number(max / $scope.maxVLines);

                                for (var u = 0; u < $scope.maxVLines; u++) {
                                    var val = (intervallRange * (u + 1));
                                    val = Number(Math.round(val * 2) / 2);
                                    val = val.toFixed(1);
                                    var per = (100 * val) / max;
                                    lines.push({
                                        'perc': per + '%',
                                        'label': val + $scope.vlinesSuffix
                                    });
                                }

                            }
                        }
                        $scope.vertcalLines = lines;
                    }
                    return $scope.vertcalLines;
                }

                $scope.getStatCount = function (stat) {
                    if ($scope.vm) {
                        var count = $scope.vm.getAttributeRules($scope.enrichment, stat).length;
                        return count;
                    } else {
                        return 0;
                    }
                }


                /**
                 * Clicked on the single row of the chart
                 * @param {*} stat 
                 */
                $scope.clicked = function (stat) {
                    if ($scope.callback) {
                        $scope.callback()(stat, $scope.enrichment);
                    }

                }
            }
        }
    });