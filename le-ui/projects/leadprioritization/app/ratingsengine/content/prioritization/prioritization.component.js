angular.module('lp.ratingsengine.wizard.prioritization', [])
    .controller('RatingsEngineAIPrioritization', function ($scope, RatingsEngineStore, RatingsEngineService, Products) {
        var vm = this;
        // angular.extend(vm, {

        // });

        vm.init = function () {
            
        }

        vm.setPredictionType = function(predictionType){
            RatingsEngineStore.setPredictionType(predictionType);
            vm.predictionType = predictionType;
            vm.validateNextStep();
        }

        vm.validateNextStep = function () {
            if (RatingsEngineStore.getPredictionType() != null) {
                vm.setValidation('prioritization', true);
            } else {
                vm.setValidation('prioritization', false);
            }
        }
        vm.setValidation = function (type, validated) {
            RatingsEngineStore.setValidation(type, validated);
        }

        vm.init();
    });