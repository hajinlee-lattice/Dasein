/**
 * config = 
 * {
        from: { name: 'from', val: undefined, position: 0, type: 'Qty' },
        to: { name: 'to', val: undefined, position: 1, type: 'Qty' }
    };
    'type' can be a subtype or a basic type like Boolean, Numerical ecc.
 */
angular
    .module('common.datacloud.query.builder.tree.edit.transaction.edit.numerical.range', [])
    .directive('numericalRangeDirective', function () {
        return {
            restrict: 'E',
            scope: {
                vm: '=',
                form: '=',
                type: '@',
                bucketrestriction: '=',
                config: '=',
                mainconfig: '=',
                showmessage: '=',
                showfrom: '=',
                showto: '=',
                changed: '&',
                fromlabel: '@',
                tolabel: '@',
                showtolabel: '@?',
                invalidcallback: '@?',
                fromdisabled: "=?",
                todisabled: "=?",
                initialvalidation: '@?'
            },

            templateUrl: '/components/datacloud/query/advanced/tree/edit/numerical-range/numerical-range.component.html',
            controller: function ($scope, $element) {

                function getConfigField(position) {
                    var values = $scope.config;
                    var config = values[Object.keys(values)[position]];
                    return config;
                }

                function validateMainConfig() {
                    $scope.conf = {};
                    if ($scope.mainconfig) {
                        var tmp = $scope.mainconfig;
                        $scope.conf.debounce = (tmp.debounce ? tmp.debounce : 500);
                    } else {
                        $scope.conf.debounce = 500;
                    }
                }

                $scope.init = function () {
                    console.log('numeric init', $scope.config, $scope.values);
                    validateMainConfig();
                    var conf = $scope.config;
                    if ($scope.initialvalidation === undefined) {
                        $scope.initialvalidation = true;
                    } else {
                        $scope.initialvalidation = !!$scope.initialvalidation;
                    }
                    if ($scope.fromdisabled === undefined) {
                        $scope.fromdisabled = false;
                    } else {
                        $scope.fromdisabled = !!$scope.fromdisabled;
                    }
                    if ($scope.todisabled === undefined) {
                        $scope.todisabled = false;
                    } else {
                        $scope.todisabled = !!$scope.todisabled;
                    }

                    $scope.values = $scope.config;
                    console.log('numeric init', $scope.config, $scope.values, $scope);
                }

                $scope.getSteps = function (position) {
                    var conf = getConfigField(position);
                    return conf.step ? conf.step : 0.1;
                }

                /**
                 * Return the min value from the model.
                 * If only one field is visible is the return value is ''
                 * @param {*} position 
                 */
                $scope.getMinVal = function (position) {
                    var conf = getConfigField(position);

                    if (!$scope.showFrom() || !$scope.showTo()) {
                        return conf.min != undefined ? conf.min : '';
                    }

                    switch (conf.position) {
                        case 0: {
                            return conf.min != undefined ? conf.min : '';
                        }
                        case 1: {
                            var fromVal = $scope.values.from.value;
                            if (fromVal) {
                                return fromVal;// + (conf.step ? conf.step : 0.1);
                            } else {
                                return conf.min != undefined ? conf.min : '';
                            }

                        }
                        default: {
                            return conf.min != undefined ? conf.min : '';
                        }
                    }

                }

                /**
                 * Return Max value from the model
                 * If only one field is visible is the return value is ''
                 * @param {*} position 
                 */
                $scope.getMaxVal = function (position) {

                    var conf = getConfigField(position);

                    if (!$scope.showFrom() || !$scope.showTo()) {
                        return conf.max != undefined ? conf.max : '';
                    }

                    switch (conf.position) {
                        case 0: {
                            var toVal = $scope.values.to.value;
                            // console.log('toVal ')
                            if (toVal) {
                                return toVal;//toVal - (conf.step ? conf.step : 0.1);
                            } else {
                                return conf.max != undefined ? conf.max : '';
                            }
                        }
                        case 1: {
                            return conf.max != undefined ? conf.max : '';
                        }
                        default: {
                            return conf.max != undefined ? conf.max : '';
                        }
                    }
                }

                /**
                 * Get the name of the input field
                 * @param {*} position 
                 */
                $scope.getName = function (position) {
                    var ret = getConfigField(position).name;
                    return ret;
                }

                /** 
                 * Return if from input lable has to be visible
                */
                $scope.showFromLabel = function () {
                    if ($scope.fromlabel) {
                        return true;
                    } else {
                        return false;
                    }
                }

                /**
                 * If 'from' input is visible
                 */
                $scope.showFrom = function () {
                    return $scope.showfrom;
                }

                /** 
                 * If label of to field has to be shown
                */
                $scope.showToLabel = function () {
                    if ($scope.tolabel) {
                        return true;
                    } else {
                        return false;
                    }
                }

                /** 
                 * If 'to' input is visible
                */
                $scope.showTo = function () {
                    return $scope.showto;
                }

                /**
                 * Based on the position and if the field is valid
                 * it changes the other input min or max
                 * @param {*} position 
                 */
                $scope.changeValue = function (position) {
                    var conf = getConfigField(position);
                    if ($scope.isValValid(position) || ($scope.invalidcallback && !!$scope.invalidcallback == true)) {
                        // console.log('Change value');
                        switch (position) {
                            case 0: {
                                var value = $scope.values.from.value;
                                // console.log('From ', value);
                                $scope.changed({ type: conf.type, position: position, value: value });
                                var toInput = $element[0].querySelector('input[name="' + getConfigField(1).name + '"]');
                                if (toInput) {
                                    toInput.min = Number(Number(value) + (conf.step ? conf.step : 0.1));
                                }
                                break;
                            }
                            case 1: {
                                var value = $scope.values.to.value;
                                // console.log('To ', value);
                                $scope.changed({ type: conf.type, position: position, value: value });
                                var fromInput = $element[0].querySelector('input[name="' + getConfigField(0).name + '"]');
                                if (fromInput) {
                                    fromInput.max = Number(Number(value) - (conf.step ? conf.step : 0.1));
                                }
                                break;
                            }
                        }
                    }
                }


                /**
                 * Check is a input field containes a valid value
                 * @param {*} position 
                 */
                $scope.isValValid = function (position) {
                    var conf = getConfigField(position);
                    var valid = true;
                    if ($scope.form[conf.name]) {
                        if ($scope.form[conf.name].$dirty === true ||
                            ($scope.form[conf.name].$dirty === false && $scope.initialvalidation === true)) {
                            valid = $scope.form[conf.name].$valid;
                        }
                    }
                    return valid;
                }

                $scope.getErrorMsg = function () {
                    if ($scope.showFrom() && $scope.showTo()) {
                        return 'Enter a valid range';
                    } else {
                        return 'Enter a valid number';
                    }
                }

                $scope.showErrorMessage = function () {
                    var ret = false;
                    var show = $scope.showmessage != undefined ? $scope.showmessage : true;

                    if (show && !$scope.isValValid(0) || !$scope.isValValid(1)) {
                        ret = true;
                    }
                    return ret;
                }

                /**
                 * Pattern applied to the attribute pattern
                 * Ex. for only integer you need to pass the value \\\d* 
                 * This is going to be transformed into \d*
                 * @param {*} position 
                 */
                $scope.getPattern = function (position) {
                    var conf = getConfigField(position);
                    if (conf.pattern !== undefined) {
                        return conf.pattern;
                    } else {
                        return '';
                    }
                }

                /**
                 * NOT USED at the moment
                 * Force the val of the field range to a valid value based on the min or max of the other field
                 * @param {*} position 
                 */
                $scope.checkValue = function (position) {
                    var conf = getConfigField(position);
                    if (!$scope.form[conf.name].$valid) {
                        var input = $element[0].querySelector('input[name="' + conf.name + '"]');
                        var min = input.min;
                        var max = input.max;
                        switch (position) {
                            case 0: {
                                conf.value = Number(max);
                                break;
                            }
                            case 1: {
                                conf.value = Number(min);
                                break;
                            }
                        }
                    } else {
                        // console.log('VALID ');
                    }
                }

                $scope.init();
            }
        }
    });