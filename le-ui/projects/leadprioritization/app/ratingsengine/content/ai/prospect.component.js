angular.module('lp.ratingsengine.ai.prospect', [])
    .controller('RatingsEngineAIProspect', function ($q, $timeout, $state, $stateParams, $scope, RatingsEngineAIStore, RatingsEngineAIService, RatingsEngineStore) {
        var vm = this;

        angular.extend(vm, {
            initializing: true,
            prospect: RatingsEngineAIStore.prospect,
            customers: RatingsEngineAIStore.customers,
            prospectPercentage: '50%',
            customersPercentage: '80%',
            buildOptions: RatingsEngineAIStore.buildOptions,
            buildOption: 0
        });

        vm.init = function () {
            $scope.$watch('vm.buildOption', function () {
                vm.setValidation('prospect', false);
                if (vm.initializing) {
                    $timeout(function () {
                        vm.initializing = false;
                        vm.setValidation('prospect', false);
                    });
                } else {
                    vm.setValidation('prospect', false);
                    alert('Changed ' + vm.buildOption);
                }
            }, true);
        }

        vm.getChartStyle = function (chartType) {
            var styleObj = {};
            if ('prospect' === chartType) {
                styleObj = {
                    'height': vm.prospectPercentage//'50%'
                }
            }
            else if ('customers' === chartType) {
                styleObj = {
                    'height': vm.customersPercentage//'50%'
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