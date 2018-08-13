angular.module('lp.models.list', [
    'mainApp.core.services.FeatureFlagService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelListTileWidget',
    'mainApp.appCommon.services.HealthService',
    'mainApp.models.modals.CopyModelFromTenantModal',
])
.controller('ModelListController', function (
    $state, $filter,
    ResourceUtility, ModelList, ModelStore, ModelService,
    CopyModelFromTenantModal, ImportModelModal, 
    FeatureFlagService, ServiceErrorUtility, HealthService
) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        models: ModelList || [],
        filteredItems: [],
        query: '',
        currentPage: 1,
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'TimeStamp',
                items: [
                    { label: 'Creation Date',   icon: 'numeric',    property: 'TimeStamp' },
                    { label: 'Model Name',      icon: 'alpha',      property: 'DisplayName' },
                    { label: 'Active Status',   icon: 'amount',     property: 'Status' },
                    { label: 'Model Type',      icon: 'alpha',      property: 'ModelType' }
                ]
            },
            filter: {
                label: 'Filter By',
                unfiltered: ModelList,
                filtered: ModelList,
                items: [
                    { label: "All", action: { }, total: 278 },
                    { label: "Active", action: { Status: 'Active' }, total: 23 },
                    { label: "Inactive", action: { Status: 'Inactive' }, total: 153 },
                    { label: "PMML", action: { ModelFileType: "PmmlModel" }, total: 36 }
                ]
            },
            create: {
                label: 'Create Model',
                //labelIcon: 'fa-plus',
                sref: 'home.models.import',
                class: 'orange-button select-label',
                icon: 'fa fa-chevron-down',
                iconclass: 'orange-button select-more',
                iconrotate: true,
                click: checkStatusBeforeCreate.bind(null, 'home.models.import')
            }
        }
    },{
        init: function() {

            this.header.create.items = [
                {
                    sref: 'home.models.import',
                    label: 'From Training Set',
                    icon: 'fa fa-file-excel-o',
                    click: checkStatusBeforeCreate.bind(null, 'home.models.import')
                },{
                    sref: 'home.models.pmml',
                    label: 'From PMML File',
                    icon: 'fa fa-file-code-o',
                    click: checkStatusBeforeCreate.bind(null, 'home.models.pmml')
                },{
                    if: 0,
                    click: vm.showCopyModelFromTenant,
                    label: 'From Another Tenant',
                    icon: 'ico ico-lattice-dots'
                }/*,{
                    if: vm.showUploadSummaryJson,
                    click: vm.importJSON,
                    label: 'Import JSON File',
                    icon: 'fa fa-file-text-o'
                }*/
            ];

            FeatureFlagService.GetAllFlags().then(function(result) {
                var flags = FeatureFlagService.Flags();
                vm.showModelCreationHistory = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
            });

            /*
            FeatureFlagService.GetAllFlags().then(function(result) {
                var flags = FeatureFlagService.Flags();

                // disable Import JSON button for now
                vm.showUploadSummaryJson = false; //FeatureFlagService.FlagIsEnabled(flags.UPLOAD_JSON);
            });
            */

            vm.processModels(vm.models);

            ModelStore.getModels().then(vm.processModels);

            ModelService.GetAllModels(false).then(function(result) {
                if (result != null && result.success === true) {
                    vm.modelsIncludingDeletedLength = result.resultObj.length;
                }
            });

        },

        showCopyModelFromTenant: function() {
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

    vm.count = function(type) {
        return $filter('filter')(vm.models, { Status: type }, true).length;
    }

    function checkStatusBeforeCreate(sref, $event) {
        $event.preventDefault();

        HealthService.checkSystemStatus().then(function() {
            $state.go(sref);
        });
    }

    vm.init();
});