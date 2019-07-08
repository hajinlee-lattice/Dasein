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
                config: '=',
                showmessage: '=',
                showfrom: '=',
                showto: '=',
                changed: '&',
                fromlabel: '@',
                tolabel: '@',
                showinline: '@',
                startplaceholder: '@',
                endplaceholder: '@',
                hidepristineerror: '@'
            },
            templateUrl: '/components/datacloud/query/advanced/tree/edit/date-range/date-range.component.html',
            controller: function ($scope, $element, $timeout, moment) {

                var DATE_FORMAT = 'YYYY-MM-DD';
                var fromPicker;
                var toPicker;

                function getConfigField(position) {
                    var values = $scope.config;
                    var config = values[Object.keys(values)[position]];
                    return config;
                }

                $scope.getConfig = function(position) {
                    var values = $scope.config;
                    var config = values[Object.keys(values)[position]];
                    return config;
                }

                $scope.vm.resetDatePicker = function() {
                    var from = document.getElementById($scope.getFromDateId());
                    if(from){
                        $scope.fromDate = undefined;
                        from.value = '';
                        fromPicker.setDate('', false);
                    }
                    var to = document.getElementById($scope.getToDateId());
                    if(to){
                        $scope.toDate = undefined;
                        to.value = '';
                        toPicker.setDate('', false);
                    }
                }


                function isDateValid(momentDate, otherPosition) {
                    switch (otherPosition) {
                        case 0: {
                            if (getConfigField(0).visible) {
                                if ($scope.fromDate) {
                                    var momentFrom = moment($scope.fromDate).format(DATE_FORMAT);
                                    var valid = moment(momentFrom).isSameOrBefore(momentDate);
                                    // console.log('Valid ==> ', valid);
                                    return valid;
                                } else {
                                    return false;
                                }
                            } else {
                                return true;
                            }
                        }
                        case 1: {
                            if (getConfigField(1).visible) {
                                if ($scope.toDate) {
                                    var momentTo = moment($scope.toDate).format(DATE_FORMAT);
                                    var valid = moment(momentDate).isSameOrBefore(momentTo);
                                    // console.log('Valid ==> ', valid);
                                    return valid;
                                } else {
                                    return false;
                                }
                            } else {
                                return true;
                            }
                        }
                        default:
                            return false;
                    }

                }

                $scope.validateDates = function() {

                    var fromConf = getConfigField(0);
                    var toConf = getConfigField(1);

                    if (fromConf.visible == true && toConf.visible && toConf.visible == true) {
                        if (!$scope.fromDate || !$scope.toDate) {
                            $scope.form[fromConf.name].$setValidity('datefrom', false);
                            $scope.form[toConf.name].$setValidity('dateto', false);
                            return;
                        }
                        var momentFrom = moment($scope.fromDate).format(DATE_FORMAT);
                        var momentTo = moment($scope.toDate).format(DATE_FORMAT);
                        if (moment(momentFrom).isSame(momentTo)) {
                            $scope.form[fromConf.name].$setValidity('datefrom', false);
                            $scope.form[toConf.name].$setValidity('dateto', false);
                        } else {
                            var valid = moment(momentFrom).isBefore(momentTo);
                            // console.log('valid', valid);
                            // console.time(fromConf.name);
                            $scope.form[fromConf.name].$setValidity('datefrom', valid);
                            $scope.form[toConf.name].$setValidity('dateto', valid);
                        }
                    } else {
                        switch (fromConf.visible) {
                            case true: {
                                if ($scope.fromDate) {
                                    $scope.form[fromConf.name].$setValidity('datefrom', true);
                                } else {
                                    $scope.form[fromConf.name].$setValidity('datefrom', false);
                                }
                                return;
                            }
                        }
                        switch (toConf.visible) {
                            case true: {
                                if ($scope.toDate) {
                                    $scope.form[toConf.name].$setValidity('dateto', true);
                                } else {
                                    $scope.form[toConf.name].$setValidity('dateto', false);
                                }
                                return;
                            }
                        }
                    }

                    setTimeout(function() {
                        $scope.$apply();
                    }, 0);
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
                }

                function initDatePicker() {
                    var fromConf = getConfigField(0);
                    if (fromConf.visible && fromConf.visible == true) {
                        var from = document.getElementById($scope.getFromDateId());

                        if (from != null) {
                            // var triggerFrom = document.getElementById($scope.getFromDateTriggerId());
                                fromPicker = new Pikaday({
                                field: from,
                                format: DATE_FORMAT,
                                onSelect: function (date) {
                                    var val = moment(date).format(DATE_FORMAT);
                                    var valid = isDateValid(val, 1);
                                    $scope.fromDate = val;
                                    $scope.validateDates();
                                    // readDate(1);
                                    if (valid) {
                                        $scope.changed({ type: 'Time', position: 0, value: val });
                                        if($scope.toDate !== undefined){
                                            $scope.changed({ type: 'Time', position: 1, value: $scope.toDate });
                                        }  
                                    }
                                }

                            });

                        }
                    }
                    var toConf = getConfigField(1);
                    if (toConf.visible && toConf.visible == true) {
                        var to = document.getElementById($scope.getToDateId());
                        if (to != null) {
                            // var triggerTo = document.getElementById($scope.getToDateTriggerId());
                                toPicker = new Pikaday({
                                field: to,
                                format: DATE_FORMAT,
                                onSelect: function (date) {
                                    var val = moment(date).format(DATE_FORMAT);
                                    var valid = isDateValid(val, 0);
                                    $scope.toDate = val;
                                    $scope.validateDates();
                                    // readDate(0);
                                    if (valid) {
                                        $scope.changed({ type: 'Time', position: 1, value: val });
                                        if($scope.fromDate !== undefined){
                                            $scope.changed({ type: 'Time', position: 0, value: $scope.fromDate });
                                        }   
                                    }
                                }
                            });

                        }
                    }

                    if ((getConfigField(0).visible && getConfigField(0).visible == true) ||
                        (getConfigField(1).visible && getConfigField(1).visible == true)) {
                        initDates();
                    }
                    $scope.validateDates();
                }


                $scope.init = function () {
                    $timeout(initDatePicker, 0);
                }

                /**
                * Get the name of the input field
                * @param {*} position 
                */
                $scope.getName = function (position) {
                    var ret = getConfigField(position).name;
                    return ret;
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

                $scope.openPicker = function(position){
                    switch(position){
                        case 0: {
                            fromPicker.show();
                            break;
                        }
                        case 1: {
                            toPicker.show();
                            break;
                        }
                    }
                }

                $scope.getErrorMsg = function(){
                    if(getConfigField(0).visible == true &&  getConfigField(1).visible == true){
                        return 'Enter a valid range';
                    }else {
                        return 'Enter a valid date';
                    }
                }

                $scope.show = function (position) {
                    var visible = getConfigField(position).visible;
                    return visible;
                }

                $scope.showErrorMessage = function(position){
                    switch(position){
                        case 0: {
                            var fromConf = getConfigField(0);
                            // console.timeEnd(fromConf.name);
                            if(fromConf.visible === true) {
                                return $scope.form[fromConf.name].$error.datefrom && ($scope.hidepristineerror ? !$scope.form[fromConf.name].$pristine : true);
                                // myForm.pw.$error.one
                            }else{
                                return false;
                            }
                        }
                        case 1: {
                            var toConf = getConfigField(1);
                            if(toConf.visible === true){
                                return $scope.form[toConf.name].$error.dateto && ($scope.hidepristineerror ? !$scope.form[toConf.name].$pristine : true);
                                // myForm.pw.$error.one
                            }else{
                                return false;
                            }
                        }

                        default:
                        return false;
                    }
                }


                $scope.init();
            }
        }
    });