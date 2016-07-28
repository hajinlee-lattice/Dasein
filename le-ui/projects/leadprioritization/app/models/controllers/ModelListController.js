angular.module('lp.models.list', [
    'mainApp.core.services.FeatureFlagService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelListTileWidget'
])
.controller('ModelListController', function (
    ResourceUtility, ModelList, ModelStore, ImportModelModal, FeatureFlagService
) {
    var vm = this;
    angular.extend(vm, {  
        ResourceUtility: ResourceUtility,
        models: ModelList || [],

        init: function() {
            FeatureFlagService.GetAllFlags().then(function(result) {
                var flags = FeatureFlagService.Flags();

                // disable Import JSON button for now
                vm.showUploadSummaryJson = false; //FeatureFlagService.FlagIsEnabled(flags.UPLOAD_JSON);
            });

            vm.processModels(vm.models);

            ModelStore.getModels().then(vm.processModels);
        },

        importJSON: function() {
            ImportModelModal.show();
        },

        processModels: function(models) {
            vm.models = models;
            vm.totalLength = models.length;
console.log(models);
            var active = models.filter(function(item) {
                return item.Status == 'Active';
            });

            var pmml = models.filter(function(item) {
                return item.ModelFileType == 'PmmlModel';
            });

            vm.activeLength = active.length;
            vm.pmmlLength = pmml.length;
        }
    });

    vm.init();
});