angular.module('lp.ratingsengine.ai.prospect', [])
    .controller('RatingsEngineAIProspect', function ($timeout, $scope, RatingsEngineAIStore, RatingsEngineStore, Prospect) {
        var vm = this;

        angular.extend(vm, {
            initializing: true,
            prospect: 0,
            customers: 0,
            prospectPercentage: '50%',
            customersPercentage: '80%',
            buildOptions: RatingsEngineAIStore.buildOptions,
            buildOption: 0
        });

        vm.init = function () {
            RatingsEngineAIStore.clearSelection();
            vm.prospect = Prospect.prospect;
            vm.customers = Prospect.customers;
            $scope.$watch('vm.buildOption', function () {
                if (vm.initializing) {
                    $timeout(function () {
                        vm.initializing = false;
                        vm.setValidation('prospect', true);
                    });
                } else {
                    //This condition is here for future implementation
                    //when all the building options are going to be enabled
                    vm.setValidation('prospect', false);
                }
            }, true);
        }

        vm.getChartStyle = function (chartType) {
            var styleObj = {};
            if ('prospect' === chartType) {
                styleObj = {
                    'height': vm.prospectPercentage
                }
            }
            else if ('customers' === chartType) {
                styleObj = {
                    'height': vm.customersPercentage
                }
                return styleObj;
            }
            return styleObj;
        }
        vm.setValidation = function(type, validated) {
            RatingsEngineStore.setValidation(type, validated);
        }
        vm.init();
    });