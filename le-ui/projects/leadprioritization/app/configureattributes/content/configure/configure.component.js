angular.module('lp.configureattributes.configure', [])
.component('configureAttributesConfigure', {
    controllerAs: 'vm',
    templateUrl: 'app/configureattributes/content/configure/configure.component.html',
    controller: function(
        $state, $stateParams, $scope, $timeout, $sce, $window,
        ResourceUtility, ConfigureAttributesStore, ModalStore
    ) {
        var vm = this,
            resolve = $scope.$parent.$resolve,
            PurchaseHistory = resolve.PurchaseHistory,
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
                    label: 'Share of Wallet',
                    description: $sce.trustAsHtml('<p>This curated attribute is calculated by comparing spend ratio for a given product of a given account with that of other accounts in the same segment.</p><p>This insignts are useful to drive sales &amp; marketing campaigns for the accounts where the share of wallet is below the desired range.</p>'),
                },
                margin: {
                    type: 'Margin',
                    label: '% Margin',
                    description: $sce.trustAsHtml('<p>This curated attribute is calculated by analyzing cost of sell &amp; revenue for a given product of a given account in the specified time window.</p><p>The insights are useful to drive sales &amp; marketing campaigns for the accounts where the profit margins are below expected levels</p>'),
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

        vm.getPeriod = function(type, data, index, append) {
            if(!type || !data) {
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

        vm.setOptions = function(form) {
            ConfigureAttributesStore.setOptions(vm.options);
            if(form) {
                vm.checkValid(form);
            }
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

        vm.goto = function(name) {
            $state.go('home.configureattributes.' + name);
        }

        vm.save = function(form) {
            ConfigureAttributesStore.saveSteps(vm.step);
            ConfigureAttributesStore.getPurchaseHistory().then(function(result) {
                vm.saveObj = result;
            });
            vm.steps[vm.step].completed = true;
            //vm.gotoNextStep(vm.step);
            
            if(form) {
                form.$setPristine();
            }
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

            if(options && valid) {
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
            var completed = ConfigureAttributesStore.getSaved(),
                required = Object.keys(vm.steps).length;
            return completed.length; // for debuging
            return (required <= completed.length);
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

        vm.validateSection = function(model) {
            if(!model) {
                return false;
            }
            for(var i in model) {
                for(var j in model[i]) {
                    if(model[i][j].Val && model[i][j].Period) {
                        return true;
                    }
                }
            }
        }

        vm.init = function() {
            var completedSteps = ConfigureAttributesStore.getSaved();
            completedSteps.forEach(function(step) {
                vm.steps[step].completed = true;
            });
            vm.steps = ConfigureAttributesStore.getSteps(ConfigureAttributesStore.purchaseHistory, vm.steps);
            vm.spendOvertime = {
                TotalSpendOvertime: vm.steps.spend_over_time.data.TotalSpendOvertime || [defaultOption],
                AvgSpendOvertime: vm.steps.spend_over_time.data.AvgSpendOvertime || [defaultOption]
            };
        };

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

            vm.modalCallback = function (args) {
                if (vm.modalConfig.dischargeaction === args.action) {
                    vm.toggleModal();
                } else if (vm.modalConfig.confirmaction === args.action) {
                    vm.toggleModal();
                }
                if(args.action === 'proceed') {
                    console.log('proceed');
                }
            }

            vm.toggleModal = function () {
                var modal = ModalStore.get(vm.modalConfig.name);
                if (modal) {
                    modal.toggle();
                }
            }

            $scope.$on("$destroy", function () {
                ModalStore.remove(vm.modalConfig.name);
            });
        }

        vm.initModalWindow();

        vm.init();
    }
});