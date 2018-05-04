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
            options: vm.options || {},
            PurchaseHistory: PurchaseHistory
        });

        vm.steps_count = Object.keys(vm.steps).length;

        vm.newArray = function(count) {
           return new Array(count).join().split(',').map(function(item, index){ return ++index;});
        }

        vm.getVals = function(type, data, index) {
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

        vm.getPeriod = function(type, data, index) {
            var index = index || 0,
                periods = data[index].periods,
                valObj = periods.find(function(item) {
                    return item.Cmp === type;
                }),
                period = valObj.Period;
            return period;
        }

        vm.setOptions = function() {
            ConfigureAttributesStore.setOptions(vm.options);
        }

        vm.addPeriod = function(array) {
            var timestamp = new Date().valueOf(),
                obj = {
                    eol: false,
                    IsEOL: false,
                    type: "PurchaseHistory",
                    created: timestamp,
                    updated: timestamp,
                    metrics: array[0].metrics,
                    periods: [{
                        Cmp: array[0].periods[0].Cmp,
                        Vals: [],
                        Period: null
                    }]
            };
            array.push(obj);
        }

        vm.removePeriod = function(array, key, index) {
            vm.spendOvertime[key] = array[key].filter(function(value, _index) { 
                return _index !== index;
            });
            console.log(vm.spendOvertime[key]);
        }

        vm.save = function(type) {
            ConfigureAttributesStore.saveOptions(type);
            ConfigureAttributesStore.getPurchaseHistory().then(function(result) {
                vm.saveObj = result;
            });
        }

        vm.enableSave = function() {
            var options = ConfigureAttributesStore.getOptions();
            return (options ? true : false);
        }

        vm.init = function() {
            vm.steps = ConfigureAttributesStore.getSteps(PurchaseHistory, vm.steps);
            vm.spendOvertime = {
                TotalSpendOvertime: vm.steps.spend_over_time.data.TotalSpendOvertime,
                AvgSpendOvertime: vm.steps.spend_over_time.data.AvgSpendOvertime
            };
        };

        vm.goto = function(name) {
            $state.go('home.configureattributes.' + name);
        }

        vm.init();
    }
});