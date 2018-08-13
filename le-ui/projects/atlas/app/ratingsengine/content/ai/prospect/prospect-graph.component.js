
angular.module('lp.ratingsengine.ai.prospect.prospect-graph', [])
    .controller('RatingsEngineAIProspectGraph', function ($scope, RatingsEngineAIStore, RatingsEngineAIService, RatingsEngineStore) {
        var vm = this;

        angular.extend(vm, {
            prospectPercentage: '50%',
            customersPercentage: '80%',
        });

        vm.init = function () {

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