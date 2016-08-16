angular.module('lp.models.list', [
    'mainApp.core.services.FeatureFlagService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelListTileWidget',
    'mainApp.models.modals.CopyModelFromTenantModal'
])
.controller('ModelListController', function (
    ResourceUtility, ModelList, ModelStore, CopyModelFromTenantModal, ImportModelModal, FeatureFlagService
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
            CopyModelFromTenantModal.show();
        },

        importJSON: function() {
            ImportModelModal.show();
        },

        processModels: function(models) {
            vm.models = models;
            vm.totalLength = models.length;

            for (var i=0; i<models.length; i++) {
                models[i].TimeStamp = Date.parse(models[i].CreatedDate);
            }
            
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