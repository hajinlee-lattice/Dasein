angular
    .module('common.datacloud.explorer.attributetile.bar.chart', [])
    .directive('barChart', function () {
        return {
            restrict: 'E',
            scope: {
                vm: '=?',
                bktlist: '=',
                callback: '&?',
                config: '=',
                statcount: '=?',
                enrichment:'=?'

            },
            templateUrl: '/components/charts/bar-chart.component.html',
            controller: function ($scope, $filter, $timeout) {

                function getHighestStat(stats, fieldName) {
                    var highest = 0;
                    stats.forEach(function (stat) {
                        if (stat[fieldName] > highest) {
                            highest = stat[fieldName];
                        }
                    })
                    return highest;
                }

                function getHorizontalPercentage(stat, field, limit) {
                    var number = stat.Cnt;

                    if (number && $scope.highest) {
                        percentage = ((number / $scope.highest) * 100);

                        if (typeof limit != 'undefined') {
                            percentage = percentage.toFixed(limit);
                        }
                        return percentage + '%';
                    }
                    return 0;
                }

                function getHorizontalPercentageSubDec(stat, field, limit) {
                    // var max = Math.ceil($scope.highest);
                    var max = Math.round($scope.highest * 2) / 2;
                    var val = stat.Lift;
                    if (max && val) {
                        var percentage = (val * 100) / max;
                        return percentage + '%';
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

                /**
                 * configuration:
                 * top: max number of rows
                 * bktlist: bucket list which containes data 
                 * color: color for the rows
                 * showfield: name field to show
                 */
                $scope.init = function () {


                    /************************************* Config ************************************************/
                    validateConfig();

                    /************************Data config ***********************/
                    $scope.tosort = $scope.config.data.tosort == undefined ? false : $scope.config.data.tosort;
                    $scope.sortBy = $scope.config.data.sortBy !== undefined ? $scope.config.data.sortBy : '-Cnt';
                    $scope.trimData = $scope.config.data.trim  !== undefined ? $scope.config.data.trim : false;
                    $scope.top = $scope.config.data.top  !== undefined ? $scope.config.data.top : 5;

                    /***********************************************************/

                    /************************** Chart Config ***********************/
                    $scope.header = $scope.config.chart.header !== undefined ? $scope.config.chart.header : 'Header';
                    $scope.emptymsg = $scope.config.chart.emptymsg !== undefined ? $scope.config.chart.emptymsg : 'No Stats';
                    $scope.color = $scope.config.chart.color !== undefined ? $scope.config.chart.color : '#D0D1D0';
                    $scope.usecolor = $scope.config.chart.usecolor!== undefined ? Boolean($scope.config.chart.usecolor) : true;
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

                    $scope.highest = 0;
                    var column = getColumnForGraph();
                    if (column !== null) {
                        $scope.highest = getHighestStat($scope.bktlist, column.field);
                    }
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
                    switch (column.type) {
                        case 'number':
                            {
                                return stat[column.field];
                            }
                        case 'string':
                            {
                                return stat[column.field] + column.suffix;
                            }
                        default:
                            return stat[column.field];
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
                    if($scope.usecolor == true || $scope.getStatCount(stat) > 0){
                        return $scope.color;
                    }else{
                        return "#939393";
                    }
                }

                $scope.getMouseOverColor = function(){
                    if($scope.mousehover){
                        return $scope.hovercolor;
                    }else{
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
                }

                $scope.getVerticalLines = function () {
                    if ($scope.bktlist.length == 0) {
                        return [];
                    }
                    if ($scope.vertcalLines === undefined) {
                        var top = Math.round($scope.highest * 2) / 2;
                        if (top == 1) {
                            $scope.maxVLines = 2;
                        }

                        var lines = [];
                        var intervalPerc = 100 / $scope.maxVLines;
                        var intervalLabel = $scope.highest / $scope.maxVLines;
                        intervalLabel = Math.round(intervalLabel * 2) / 2;
                        for (var i = 0; i < $scope.maxVLines; i++) {
                            var perc = (intervalPerc * (i + 1));
                            var label = (intervalLabel * (i + 1));
                            lines.push({
                                'perc': perc + '%',
                                'label': label + $scope.vlinesSuffix
                            });
                        }
                        $scope.vertcalLines = lines;
                    }
                    return $scope.vertcalLines;
                }

                $scope.getStatCount = function(stat){
                    if($scope.vm){
                        var count = $scope.vm.getAttributeRules($scope.enrichment, stat).length;
                        return count;
                    }else{
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