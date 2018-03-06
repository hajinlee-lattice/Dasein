/**
 * config = 
 * {
        from: { name: 'from', initial: undefined, position: 0, type: 'Time', visible: true },
        to: { name: 'to', initial: undefined, position: 1, type: 'Time', visible: false }
    };
    'type' can be a subtype or a basic type like Boolean, Numerical ecc.
 */
angular
    .module('common.datacloud.query.builder.tree.edit.transaction.edit.date.range', ['angularMoment'])
    .directive('dateRangeDirective', function () {
        return {
            restrict: 'E',
            scope: {
                vm: '=',
                form: '=',
                type: '@',
                bucketrestriction: '=',
                config: '@',
                showmessage: '=',
                showfrom: '=',
                showto: '=',
                changed: '&',
                fromlabel: '@',
                tolabel: '@'

            },
            templateUrl: '/components/datacloud/query/advanced/tree/edit/date-range/date-range.component.html',
            controller: function ($scope, $element, $timeout, moment) {

                var DATE_FORMAT = 'YYYY-MM-DD';

                function getConfigField(position) {
                    var values = JSON.parse($scope.config);
                    var config = values[Object.keys(values)[position]];
                    return config;
                }

                function initDates() {
                    var fromDate = getConfigField(0).initial;
                    if (fromDate != undefined) {
                        $scope.fromDate = moment(getConfigField(0).initial).format(DATE_FORMAT);
                    }
                    var toDate = getConfigField(1).initial;
                    if (toDate != undefined) {
                        $scope.toDate = moment(getConfigField(1).initial).format(DATE_FORMAT);
                    }

                    // $scope.toDate = moment(getConfigField(1).initial).format(DATE_FORMAT);
                    console.log('Values', $scope.fromDate, ' == ', $scope.toDate);
                }

                function initDatePicker() {
                    // console.log('Values', getConfigField(0).visible, ' == ', getConfigField(1).visible);
                    // console.log('INITI OHOHOHOHOH');
                    console.log(JSON.parse($scope.config));
                    if (getConfigField(0).visible && getConfigField(0).visible == true) {
                        var from = document.getElementById($scope.getFromDateId());

                        if (from != null) {
                            var triggerFrom = document.getElementById($scope.getFromDateTriggerId());
                            var fromPicker = new Pikaday({
                                field: from,
                                trigger: triggerFrom,
                                format: DATE_FORMAT,
                                keyboardInput: false,
                                setDate: function () {

                                },
                                onSelect: function (date) {
                                    var val = moment(date).format(DATE_FORMAT);
                                    $scope.changed({ type: 'Time', position: 0, value: val });
                                    $scope.fromDate = val;
                                }

                            });

                        }
                    }
                    if (getConfigField(1).visible && getConfigField(1).visible == true) {
                        var to = document.getElementById($scope.getToDateId());
                        if (to != null) {
                            var triggerTo = document.getElementById($scope.getToDateTriggerId());
                            var toPicker = new Pikaday({
                                field: to,
                                format: DATE_FORMAT,
                                trigger: triggerTo,
                                onSelect: function (date) {
                                    var val = moment(date).format(DATE_FORMAT);
                                    $scope.changed({ type: 'Time', position: 1, value: val });
                                    $scope.toDate = val;
                                }
                            });

                        }
                    }

                    if ((getConfigField(0).visible && getConfigField(0).visible == true) ||
                        (getConfigField(1).visible && getConfigField(1).visible == true)) {
                        initDates();
                    }
                }



                $scope.init = function () {
                    $timeout(initDatePicker, 0);
                }

                $scope.getFromDateId = function () {
                    var id = $scope.bucketrestriction.attr;
                    return id + '.txn_from';
                }
                $scope.getFromDateTriggerId = function () {
                    var id = $scope.bucketrestriction.attr;
                    return id + '.txn_from_trigger';
                }

                $scope.getToDateId = function () {
                    var id = $scope.bucketrestriction.attr;
                    return id + '.txn_to';
                }

                $scope.getToDateTriggerId = function () {
                    var id = $scope.bucketrestriction.attr;
                    return id + '.txn_to_trigger';
                }

                $scope.show = function (position) {
                    var visible = getConfigField(position).visible;
                    // console.log('Position ', position, ' Visible: ', visible);
                    return visible;
                    // if (visible && visible == false) {
                    //     return false;
                    // } else {
                    //     return true;
                    // }
                }




                $scope.init();
            }
        }
    });