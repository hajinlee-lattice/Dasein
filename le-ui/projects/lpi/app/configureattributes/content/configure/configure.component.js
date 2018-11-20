angular.module('lp.configureattributes.configure', [])
.component('configureAttributesConfigure', {
    controllerAs: 'vm',
    templateUrl: 'app/configureattributes/content/configure/configure.component.html',
    controller: function(
        $state, $stateParams, $scope, $timeout, $sce, $window, $q,
        ResourceUtility, ConfigureAttributesStore, Modal, StateHistory
    ) {
        var vm = this,
            forceNextStep = false,
            resolve = $scope.$parent.$resolve,
            PurchaseHistory = resolve.PurchaseHistory,
            Precheck = resolve.Precheck,
            totalMonths = 60,
            timestamp = + new Date(),
            defaultOption = {
                metrics: null,
                periods: [{
                    Cmp: null,
                    Vals: [0],
                    Period: null
                }],
                type: "PurchaseHistory",
                created: timestamp,
                updated: timestamp,
                eol: false,
                IsEOL: false
            };

        angular.extend(vm, {
            stateParams: $stateParams,
            steps: {
                spend_change: {
                    type: 'SpendChange',
                    label: '% Spend Change',
                    description: $sce.trustAsHtml('<p>This curated attribute is calculated by comparing average spend for a given product of a given account in the specified time window with that of the range in prior time window.</p><p>The insights are useful to drive sales &amp; marketing campaigns for the accounts where the spend is declining.</p>'),
                },
                spend_over_time: {
                    type: 'TotalSpendOvertime,AvgSpendOvertime',
                    label: 'Spend Over Time',
                    description: $sce.trustAsHtml('<p>This curated attribute is calulated by aggregating spend for a given product of a given account over a specified time window</p><p>The insights are useful to provide dynamic talking points to the reps when executing x-sell &amp; upsell plays for a set of accounts.</p>'),
                },
                share_of_wallet: {
                    type: 'ShareOfWallet',
                    label: '% Share of Wallet',
                    disabled: Precheck.disableShareOfWallet,
                    description: $sce.trustAsHtml('<p>This curated attribute is calculated by comparing spend ratio for a given product of a given account with that of other accounts in the same segment.</p><p>This insignts are useful to drive sales &amp; marketing campaigns for the accounts where the share of wallet is below the desired range.</p>'),
                },
                margin: {
                    type: 'Margin',
                    label: '% Margin',
                    disabled: Precheck.disableMargin,
                    description: $sce.trustAsHtml('<p>This curated attribute is calculated by analyzing cost of sell &amp; revenue for a given product of a given account in the specified time window.</p><p>The insights are useful to drive sales &amp; marketing campaigns for the accounts where the profit margins are below expected levels</p>'),
                }
            },
            step: $state.current.name.split('.').pop(-1),
            periods: {
                Week: totalMonths * 4,
                Month: totalMonths,
                Quarter: totalMonths / 3 ,// 3 quaters a month
                Year: totalMonths / 12
            },
            periodsOptions: [],
            options: ConfigureAttributesStore.getOptions() || {},
            completed: ConfigureAttributesStore.getSaved(),
            PurchaseHistory: PurchaseHistory,
            hasChanges: false,
            disabledObj: {},
            precheck: Precheck
        });

        vm.steps_count = Object.keys(vm.steps).length;

        vm.log = function(log) {
            console.log(log);
        }

        vm.newArray = function(count) {
           return new Array(count).join().split(',').map(function(item, index){ return ++index;});
        }

        vm.getVals = function(type, data, index) {
            if(!type || !data) {
                return false;
            }
            var index = index || 0,
                periods = data[index].periods,
                valObj = periods.find(function(item) {
                    return item.Cmp === type;
                }),
                val;
            if (type === 'WITHIN') {
                val = (valObj ? valObj.Vals[0] : null);
            } else if (type === 'BETWEEN') {
                var max = Math.max.apply(null, valObj.Vals),
                    min = Math.min.apply(null, valObj.Vals);
                val = max - min + 1;
            }
            return val;
        }

        vm.getPeriod = function(type, data, index) {
            if(!type || !data) {
                return false;
            }

            var index = index || 0,
                periods = data[index].periods,
                valObj = periods.find(function(item) {
                    return item.Cmp === type;
                }),
                period = valObj.Period;

            return period;
        }

        vm.setOptions = function(form) {
            ConfigureAttributesStore.setOptions(vm.options);
            if(form) {
                vm.checkValid(form);
            }
        }

        vm.setDisabled = function(key, debug) {
            var options = vm.options[key];

            vm.disabledObj[key] = {};

            for(var i in options) {
                var first_key = Object.keys(options[i])[0],
                    option = options[i][first_key];
                if(!vm.disabledObj[key][option.Period]) {
                    vm.disabledObj[key][option.Period] = [];
                }
                vm.disabledObj[key][option.Period].push(option.Val);
            }
        }

        vm.disableSpendOvertime = function(option, period, value) {
            if(vm.disabledObj[option] && vm.disabledObj[option][period]) {
                return vm.disabledObj[option][period].indexOf(""+value) >= 0;
            }
            return false;
        }

        vm.addPeriod = function(array, type, form) {
            var timestamp = new Date().valueOf(),
                obj = {
                    eol: false,
                    IsEOL: false,
                    type: "PurchaseHistory",
                    created: timestamp,
                    updated: timestamp,
                    metrics: null,
                    periods: [{
                        Cmp: null,
                        Vals: [],
                        Period: null
                    }]
            };

            array.push(obj);

            ConfigureAttributesStore.setOptions(vm.options);

            vm.makeSpendOverTimes(type);

            if(form) {
                form.$setDirty();
            }
        }

        vm.spendOverTimes = {};
        vm.makeSpendOverTimes = function(type) {
            vm.spendOverTimes[type] = vm.spendOverTimes[type] || {};
            for(var i in vm.options[type]) {
                vm.spendOverTimes[type]['periods'] = vm.spendOverTimes[type]['periods'] || {};
                for(var j in vm.options[type][i]) {
                    var val = vm.options[type][i][j].Val,
                        period = vm.options[type][i][j].Period;

                    vm.spendOverTimes[type]['periods'][period] = vm.spendOverTimes[type]['periods'][period] || [];

                    if(vm.spendOverTimes[type]['periods'][period].indexOf(val) === -1) {
                        vm.spendOverTimes[type]['periods'][period].push(val);
                    }
                }
            }
        }

        /**
         * Used to rebuild the model because the ngRepeat doesn't like 
         * it if you delete objects from it's array and the indexes are
         * weird or something
         */
        var rebuildModel = function(key) {
            var newOptions = [];
            for(var i in vm.options[key]) {
                var newCmp = [];
                for(var j in vm.options[key][i]) {
                    newCmp[j] = vm.options[key][i][j];
                }
                newOptions.push(newCmp);
            }
            vm.options[key] = newOptions;
        }

        vm.removePeriod = function(array, key, index, form) {
            var tmp = array[key].filter(function(value, _index) { 
                    return _index !== index;
                });

            vm.spendOvertime[key] = tmp;

            delete vm.options[key][index];

            ConfigureAttributesStore.setOptions(vm.options);

            rebuildModel(key);

            if(form) {
                form.$setDirty();
            }
        }

        vm.nextStep = function(step) {
            var steps = Object.keys(vm.steps),
                currentIndex = steps.indexOf(step);

            if(steps[currentIndex + 1]) {
                return steps[currentIndex + 1];
            }
            return null;
        }

        vm.gotoNextStep = function(step) {
            var nextStep = vm.nextStep(step);
            if(nextStep) {
                $state.go('home.configureattributes.' + nextStep);
            }

        }

        vm.goto = function(name, item) {
            if(item.disabled) {
                return false;
            }
            if(!forceNextStep) {
                $state.go('home.configureattributes.' + name);
            } else {
                // this doesn't really work when you have null stats, because how would you get to % Margin if you can't configure Share of Wallet?
                var completed = ConfigureAttributesStore.getSaved(),
                    steps = Object.keys(vm.steps),
                    previousStep = steps[steps.indexOf(name) - 1];

                if(steps.indexOf(name) === 0 || vm.steps[name].completed || (vm.steps[previousStep] && vm.steps[previousStep].completed)) {
                    $state.go('home.configureattributes.' + name);
                }
            }
        }

        vm.save = function(form) {
            vm.hasChanges = true;
            ConfigureAttributesStore.saveSteps(vm.step);
            ConfigureAttributesStore.getPurchaseHistory().then(function(result) {
                vm.saveObj = result;
            });
            vm.steps[vm.step].completed = true;
            if(forceNextStep) {
                vm.gotoNextStep(vm.step);
            }
            
            if(form) {
                form.$setPristine();
            }
        }

        vm.submit = function() {
            ConfigureAttributesStore.savePurchaseHistory(vm.step).then(function() {
                vm.hasChanges = false;
                $state.go('home.configureattributes.done');
            });
        }

        vm.enableSave = function(form) {
            var options = ConfigureAttributesStore.getOptions(),
                hasOptions = false;

            if(form) {
                var valid = form.$valid, 
                    dirty = form.$dirty;
            }

            if(options && valid) {
                var types = vm.steps[vm.step].type.split(',');
                types.forEach(function(type, key) {
                    if(options[type] && dirty) {
                        hasOptions = true;
                    }
                });
            }

            if(vm.step === 'spend_over_time') {
                var types = vm.steps[vm.step].type.split(',');
                for(var i in types) {
                    var type = types[i],
                        option = vm.options[type];
                    for(var j in option) {
                        for(var k in option[j]) {
                            if(Object.keys(option[j][k]).length >= 2) {
                                hasOptions = true;
                            }
                        }
                    }
                }
            }

            return (hasOptions ? true : false);
        }

        vm.enableSubmit = function() {
            var completed = ConfigureAttributesStore.getSaved(),
                required = Object.keys(vm.steps).length;

            return completed.length;
            //return (required <= completed.length); // works but design requirements don't
        }

        var getObj = function(path, obj) {
            return path.split('.').reduce(function(obj, i) {
                if(obj && obj[i]) {
                    return obj[i];
                }
            }, obj);
        }

        var setObj = function (path, value, scope) {
            var levels = path.split('.'),
            max_level = levels.length - 1,
            target = scope;

            levels.some(function (level, i) {
                if (typeof level === 'undefined') {
                    return true;
                }
                if (i === max_level) {
                    target[level] = value;
                } else {
                    var obj = target[level] || {};
                    target[level] = obj;
                    target = obj;
                }
            });
        }

        vm.initValue = function(path, value) {
            var model = getObj(path, vm.options),
                keys = path.split('.'),
                key = keys[0],
                obj = {};
            if(value) {
                setObj(path, value, obj);
                angular.merge(vm.options, obj);
            }
        }

        vm.checkValidDelay = function(form) {
            $timeout(function() {
                vm.checkValid(form);
            }, 1);
        };

        vm.checkValid = function(form) {
            //console.log(form.$valid);
        }

        vm.validateSendOvertime = function(name, debug) {
            if(debug) {
                console.group();
                console.log('validateSendOvertime', 'start', name, vm.options[name]);
            }
            var model = vm.options[name] || {},
                spendOvertime = vm.spendOvertime[name] || [];
            if(!model) {
                if(debug) {
                    console.log('if(!model)');
                    console.groupEnd();
                }
                return false;
            }
            if(Object.keys(model).length !== spendOvertime.length) {
                if(debug) {
                    console.log('if(Object.keys(model).length !== spendOvertime.length)');
                    console.groupEnd();
                }
                return false;
            }
            var valid = [];
            for(var i in model) {
                for(var j in model[i]) {
                    if(debug) {
                        console.log('for(var j in model[i])', i, model[i]);
                    }
                    if(model[i][j].Val && model[i][j].Period) {
                        if(debug) {
                            console.log('validateSendOvertime', 'if(model[i][j].Val && model[i][j].Period)', model[i][j]);
                        }
                        valid.push(true);
                    } else {
                        if(debug) {
                            console.log('if(model[i][j].Val && model[i][j].Period) -- else', model[i][j]);
                        }
                        valid.push(false);
                    }
                }
            }
            if(debug) {
                console.log('validateSendOvertime', 'completed', '(valid.indexOf(false) === -1)', (valid.indexOf(false) === -1), valid);
                console.groupEnd();
            }
            return (valid.indexOf(false) === -1);
        }

        vm.dataCheckWrapper = function(data) {
            return true; //subverting for PLS-8323
            return (data);
        }

        vm.uiCanExit = function(Transition) {
            var deferred = $q.defer();

            var goingTo = Transition.targetState().identifier(),
                goingToBase = goingTo.substr(0, goingTo.lastIndexOf(".")),
                comingFrom = $state.current.name,model,
                comingFromBase = comingFrom.substr(0, comingFrom.lastIndexOf("."));

            if(goingToBase === comingFromBase || !vm.hasChanges) {
                deferred.resolve(true);
            } else {
                vm.toggleModal();

                vm.modalCallback = function(args) {
                    if(args.action === 'proceed') {
                        deferred.resolve(true);
                    } else {
                        vm.toggleModal();
                        deferred.resolve(false);
                    }
                }
            }
            return deferred.promise;
        }

        vm.initModalWindow = function () {
            vm.modalConfig = {
                'name': "configure_attributes",
                'type': 'sm',
                'title': 'Warning',
                'titlelength': 100,
                'dischargetext': 'Cancel',
                'dischargeaction': 'cancel',
                'confirmtext': 'Yes, Confirm',
                'confirmaction': 'proceed',
                'icon': 'fa fa-exclamation-triangle',
                'iconstyle': {'color': 'white'},
                'confirmcolor': 'blue-button',
                'showclose': true,
                'headerconfig': {'background-color':'#FDC151', 'color':'white'},
                'confirmstyle' : {'background-color':'#FDC151'}
            };

            vm.toggleModal = function () {
                var modal = Modal.get(vm.modalConfig.name);
                if (modal) {
                    modal.toggle();
                }
            }

            $scope.$on("$destroy", function () {
                Modal.remove(vm.modalConfig.name);
            });
        }

        var makePeriodsObject = function(periods) {
            vm.periodsOptions = [];
            for(var i in periods) {
                var key = i,
                    total = periods[key],
                    period = {
                        label: key + 's',
                        value: key
                    };
                vm.periodsOptions.push(period);
            }
        }

        vm.$onInit = function() {
            vm.initModalWindow();
            makePeriodsObject(vm.periods);
            var completedSteps = ConfigureAttributesStore.getSaved(),
                totalSpendOvertimeOptionsAr,
                avgSpendOvertimeOptionsAr;

            completedSteps.forEach(function(step) {
                vm.steps[step].completed = true;
            });

            vm.steps = ConfigureAttributesStore.getSteps(ConfigureAttributesStore.purchaseHistory, vm.steps);

            // I changed behavior with setting options and this now breaks Spend Over Time
            // when you go switch to that page after setting options
            // 
            // if(vm.options.TotalSpendOvertime) {
            //     totalSpendOvertimeOptionsAr = [];
            //     for(var i in vm.options.TotalSpendOvertime) {
            //         var option = angular.copy(vm.options.TotalSpendOvertime[i]);
            //         totalSpendOvertimeOptionsAr.push(option);
            //     }
            // }
            
            // if(vm.options.AvgSpendOvertime) {
            //     avgSpendOvertimeOptionsAr = [];
            //     for(var i in vm.options.AvgSpendOvertime) {
            //         var option =  angular.copy(vm.options.AvgSpendOvertime[i]);
            //         avgSpendOvertimeOptionsAr.push(option);
            //     }
            // }

            vm.spendOvertime = {
                TotalSpendOvertime: totalSpendOvertimeOptionsAr || vm.steps.spend_over_time.data.TotalSpendOvertime || [defaultOption],
                AvgSpendOvertime: avgSpendOvertimeOptionsAr || vm.steps.spend_over_time.data.AvgSpendOvertime || [defaultOption]
            };
        }
    }
});