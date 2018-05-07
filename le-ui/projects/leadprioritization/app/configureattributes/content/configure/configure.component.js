angular.module('lp.configureattributes.configure', [])
.component('configureAttributesConfigure', {
    controllerAs: 'vm',
    templateUrl: 'app/configureattributes/content/configure/configure.component.html',
    controller: function(
        $state, $stateParams, $scope, $timeout, 
        ResourceUtility, ConfigureAttributesStore
    ) {
        var vm = this,
            resolve = $scope.$parent.$resolve,
            PurchaseHistory = resolve.PurchaseHistory,
            totalMonths = 60;

        angular.extend(vm, {
            stateParams: $stateParams,
            steps: {
                spend_change: {
                    type: 'SpendChange',
                    label: '% Spend Change'
                },
                spend_over_time: {
                    type: 'TotalSpendOvertime,AvgSpendOvertime',
                    label: 'Spend Over Time'
                },
                share_of_wallet: {
                    type: 'ShareOfWallet',
                    label: 'Share of Wallet'
                },
                margin: {
                    type: 'Margin',
                    label: '% Margin'
                }
            },
            step: $state.current.name.split('.').pop(-1),
            periods: {
                Weeks: totalMonths * 4,
                Months: totalMonths,
                Years: totalMonths / 12,
                Quarters: totalMonths / 3 // 3 quaters a month
            },
            options: ConfigureAttributesStore.getOptions() || {},
            completed: ConfigureAttributesStore.getSaved(),
            PurchaseHistory: PurchaseHistory
        });

        vm.steps_count = Object.keys(vm.steps).length;

        vm.newArray = function(count) {
           return new Array(count).join().split(',').map(function(item, index){ return ++index;});
        }

        vm.getVals = function(type, data, index) {
            if(!type) {
                return false;
            }
            var index = index || 0,
                periods = data[index].periods,
                valObj = periods.find(function(item) {
                    return item.Cmp === type;
                }),
                val;
            if (type === 'WITHIN') {
                val = valObj.Vals[0];
            } else if (type === 'BETWEEN') {
                var max = Math.max(...valObj.Vals),
                    min = Math.min(...valObj.Vals);
                val = max - min + 1;
            }
            return val;
        }

        vm.getPeriod = function(type, data, index, append) {
            if(!type) {
                return false;
            }
            var index = index || 0,
                periods = data[index].periods,
                valObj = periods.find(function(item) {
                    return item.Cmp === type;
                }),
                period = valObj.Period;
            return period + (period.slice(-1) !== 's' ? append : '');
        }

        vm.setOptions = function() {
            ConfigureAttributesStore.setOptions(vm.options);
        }

        vm.addPeriod = function(array, form) {
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

            if(form) {
                form.$setDirty();
            }
        }

        vm.removePeriod = function(array, key, index, form) {
            var tmp = array[key].filter(function(value, _index) { 
                return _index !== index;
            });
            vm.spendOvertime[key] = tmp;
            delete vm.options[key][index];

            ConfigureAttributesStore.setOptions(vm.options);

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

        vm.goto = function(name) {
            $state.go('home.configureattributes.' + name);
        }

        vm.save = function() {
            ConfigureAttributesStore.saveSteps(vm.step);
            ConfigureAttributesStore.getPurchaseHistory().then(function(result) {
                vm.saveObj = result;
            });
            vm.steps[vm.step].completed = true;
            //vm.gotoNextStep(vm.step);
        }

        vm.submit = function() {
            ConfigureAttributesStore.savePurchaseHistory(vm.step).then(function() {
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

            if(options) {
                var types = vm.steps[vm.step].type.split(',');
                types.forEach(function(type, key) {
                    if(options[type] && dirty) {
                        hasOptions = true;
                    }
                });
            }

            return (hasOptions ? true : false);
        }

        vm.enableSubmit = function() {
            var completed = ConfigureAttributesStore.getSaved();
            return completed.length;
        }

        vm.checkValid = function(form) {
            //console.log(form);
        }

        vm.init = function() {
            var completedSteps = ConfigureAttributesStore.getSaved();
            completedSteps.forEach(function(step) {
                vm.steps[step].completed = true;
            });
            vm.steps = ConfigureAttributesStore.getSteps(ConfigureAttributesStore.purchaseHistory, vm.steps);
            vm.spendOvertime = {
                TotalSpendOvertime: vm.steps.spend_over_time.data.TotalSpendOvertime,
                AvgSpendOvertime: vm.steps.spend_over_time.data.AvgSpendOvertime
            };
        };

        vm.init();
    }
});