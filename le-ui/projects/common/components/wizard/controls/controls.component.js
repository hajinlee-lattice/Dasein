angular.module('common.wizard.controls', [])
.controller('ImportWizardControls', function(
    $state, $stateParams, $scope, $timeout, $transitions, $rootScope, $window, 
    ResourceUtility, WizardProgressItems, WizardProgressContext, WizardControlsOptions, WizardValidationStore, StateHistory
    // ImportWizardService, ImportWizardStore
) {
    var vm = this,
        ImportWizardControls = this,
        preventUnload = WizardControlsOptions.preventUnload;


    angular.extend(vm, {
        itemMap: {},
        items: WizardProgressItems,
        state: $state.current.name,
        prev: WizardControlsOptions.backState,
        next: function() {
            return 'home.' + WizardProgressContext;
        }(),
        valid: false,
        toState: $state.current,
        nextDisabled: false,
        prevDisabled: false 
    });

    $transitions.onStart({}, function(trans) {
        var to = trans.$to(),
            params = trans.params('to'),
            from = trans.$from();

        angular.element(window).scrollTop(0,0);
        
        vm.toState = to;
        vm.item = vm.itemMap[to.name];
    });

    $transitions.onFinish({}, function() {
        vm.nextDisabled = false;
    });

    if(preventUnload) {
        /**
         * leaving this here because it's interesting
         */
        // if($window.performance) {
        //     console.log('$window.performance', $window.performance.navigation.type, {
        //         TYPE_RELOAD: $window.performance.navigation.TYPE_RELOAD, 
        //         TYPE_NAVIGATE: $window.performance.navigation.TYPE_NAVIGATE, 
        //         TYPE_BACK_FORWARD: $window.performance.navigation.TYPE_BACK_FORWARD
        //     });
        // }
        
        /**
         * if there is no last from we assume the user has manually reloaded the page
         * so we give them a chance to no reload and if they proceed anyway we take
         * them back to the play list
         */
        if(!StateHistory.lastFrom().name) {
            if(preventUnload === true) {
                $state.go('home');
            } else {
                $state.go(preventUnload);
            }
        } else {
            $window.onbeforeunload = function(event) {
                var warning = 'Changes you made may not be saved. Are you sure?'; // this is just the default messaging which can't be changed in chrome anyway
                event.returnValue = warning;
                return warning;
            };
        }
    }

    $scope.$on("$destroy", function(){
        $window.onbeforeunload = null;
    });

    this.historyStore = this.historyStore || {};

    vm.init = function() {
        vm.rootState = vm.next + '.';
        vm.setButtons();

        if (WizardControlsOptions.secondaryLink) {
            vm.secondaryLink = true;
        };

        if (WizardControlsOptions.secondaryLinkValidation) {
            vm.secondaryLinkValidation = true;
        };

        vm.items.forEach(function(item) {
            var key = vm.rootState + item.state;
            vm.itemMap[key] = item;
        });
        vm.item = vm.itemMap[vm.toState.name];

        ImportWizardControls.setHistoryStore($state.current.name);
    }

    this.setHistoryStore = function(path) {
        var prevState = vm.prev,
            prevParams = null;
        if(StateHistory.lastFrom() && StateHistory.lastFrom().name && !vm.prev) {
            var prevState = StateHistory.lastFrom(),
                prevParams = StateHistory.lastFromParams();
        }
        ImportWizardControls.historyStore[path] = {
            prev: {
                state: prevState, 
                params: prevParams
            }
        };
    }

    this.getHistoryStore = function(path) {
        if(path) {
            return ImportWizardControls.historyStore[path];
        }
        return ImportWizardControls.historyStore;
    }

    vm.click = function(isPrev) {
        vm.setButtons();

        if (vm.next && !isPrev) {
            vm.go(vm.next, isPrev);
        } else if (isPrev && vm.prev) {
            var hasParams = vm.prev.route ? true : false;

            if (hasParams){
                vm.go(vm.prev.route, isPrev, vm.prev.params);    
            } else {
                vm.go(vm.prev, isPrev);
            }
        } else if (isPrev && !vm.prev) {
            var storedState = ImportWizardControls.getHistoryStore($state.current.name);
            if(storedState && storedState.prev && storedState.prev.state) {
                $state.go(storedState.prev.state, storedState.prev.params);
            } else {
                //window.history.back();
                //$state.go($uiRouter.globals.$current.parent.navigable);
                $state.go('home');
            }
        } else if (!isPrev && !vm.next) {
            if (WizardControlsOptions.nextState) {
                var params = WizardControlsOptions.nextStateParams
                    ? typeof WizardControlsOptions.nextStateParams == 'function'
                        ? WizardControlsOptions.nextStateParams()
                        : WizardControlsOptions.nextStateParams
                    : {};

                vm.go(WizardControlsOptions.nextState, isPrev, params);
            }
        }

    }

    vm.clickSecondary = function(state, params) {
        var current = vm.itemMap[$state.current.name];
        if(current.secondaryFn) {
            current.secondaryFn(state, params);
        } else {
            $state.go(vm.item.secondaryLink);
        }
    }

    vm.go = function(state, isPrev, params) {
        var current = vm.itemMap[$state.current.name];
        vm.nextDisabled = true;

        if (current.nextFn && !isPrev) {
            current.nextFn(state, params);
        } else {
            $state.go(state, params);
        }
    }

    vm.setButtons = function() {
        var current = $state.current.name,
            item, state, split, last, prev, next, nsplit, psplit;
        
        for (var i=0; i<vm.items.length; i++) {
            item = vm.items[i];
            state = item.state;
            
            if (vm.rootState + state == current) {
                split = state.split('.');
                last = split[split.length-1];

                vm.prev = WizardControlsOptions.backState;
                vm.next = '';
                
                if (i+1 < vm.items.length) {
                    next = vm.getNext(i).state;
                    nsplit = next.split('.');

                    vm.next = vm.rootState + nsplit.join('.');
                }

                if (i-1 >= 0) {
                    prev = vm.getPrev(i-1) ? vm.getPrev(i-1).state : vm.prev;
                    psplit = prev.split('.');
                    
                    vm.prev = vm.rootState + psplit.join('.');
                }
            }

        }

        vm.isValid();
    }

    vm.getNext = function(index) {
        var item,
            i = 1;

        while (item = vm.items[index + i++]) {
            if (item.hide && index + i < vm.items.length) {
                continue;
            }

            break;
        }
        return item;
    }

    vm.getPrev = function(index) {
        var item,
            i = 0;

        while (item = vm.items[index + i]) {
            i = i - 1;

            if (item.hide && index + i >= 0) {
                continue;
            }

            break;
        }
        return item;
    }

    vm.isValid = function() {
        var current = $state.current.name,
            currentStep = current.split('.').pop();

        if (WizardValidationStore.getValidation) {
            vm.valid = WizardValidationStore.getValidation(currentStep);
            if(vm.item && vm.item.afterNextValidation) {
                vm.nextDisabled = !vm.valid;
            }
        } else {
            vm.valid = true;
        }

        return vm.valid;
    }

    vm.init();
});