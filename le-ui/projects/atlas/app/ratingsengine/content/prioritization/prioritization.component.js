angular.module('lp.ratingsengine.wizard.prioritization', [])
    .controller('RatingsEngineAIPrioritization', function ($scope, $stateParams, RatingsEngineStore, RatingsEngineService, Products, PredictionType) {
        var vm = this;
        angular.extend(vm, {
            predictionType: PredictionType
        });

        vm.init = function () {
            vm.validateNextStep();
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