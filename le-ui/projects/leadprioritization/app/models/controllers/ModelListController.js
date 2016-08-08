angular.module('lp.models.list', [
    'mainApp.core.services.FeatureFlagService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelListTileWidget',
    'mainApp.models.modals.CopyModelModal'
])
.controller('ModelListController', function (
    ResourceUtility, ModelList, ModelStore, CopyModelModal, ImportModelModal, FeatureFlagService
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

        copyModel: function() {
            CopyModelModal.show();
        },

        importJSON: function() {
            ImportModelModal.show();
        },

        processModels: function(models) {
            vm.models = models;
            vm.totalLength = models.length;
            
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