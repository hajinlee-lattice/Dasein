angular.module('common.attributes.enable', [])
.config(function($stateProvider) {
    $stateProvider
        .state('home.attributes.enable', {
            url: '/enable/:section/:category/:subcategory',
            params: {
                section: {
                    dynamic: false,
                    value: 'Segmentation'
                },
                category: {
                    dynamic: false,
                    value: 'Website Profile'
                },
                subcategory: {
                    dynamic: true,
                    value: ''
                }
            },
            onExit: ['AttrConfigStore', function(AttrConfigStore) {
                AttrConfigStore.init();
            }],
            resolve: {
                overview: ['$q', 'AttrConfigService', function($q, AttrConfigService) {
                    var deferred = $q.defer();
                    
                    AttrConfigService.getOverview('usage').then(function(response) {
                        deferred.resolve(response.data || []);
                    });

                    return deferred.promise;
                }],
                config: ['$q', '$stateParams', 'AttrConfigService', 'AttrConfigStore', function($q, $stateParams, AttrConfigService, AttrConfigStore) {
                    var deferred = $q.defer();
                    var section = $stateParams.section;
                    
                    AttrConfigService.getConfig('usage', $stateParams.category, { usage: section }).then(function(response) {
                        AttrConfigStore.setData('config', response.data || []);
                        deferred.resolve(response.data || []);
                    });

                    return deferred.promise;
                }]
            },
            views: {
                "subsummary@": "attrSubheader",
                "main@": "attrEnable"
            }
        });
})
.component('attrEnable', {
    templateUrl: '/components/datacloud/attributes/enable/enable.component.html',
    bindings: {
        overview: '<',
        config: '<'
    },
    controller: function($stateParams, AttrConfigStore) {
        var vm = this;

        vm.filters = AttrConfigStore.getFilters();
        vm.store = AttrConfigStore;
        vm.isSaving = false;

        vm.$onInit = function() {
            console.log('init attrEnable', vm);

            vm.data = vm.store.getData();
            vm.section = vm.store.getSection();
            vm.params = $stateParams;
            vm.categories = vm.overview.AttrNums;
            vm.tabs = AttrConfigStore.getTabMetadata(vm.section);
            
            vm.store.setLimit(vm.getTabLimit(vm.params.section));
        };

        vm.getTabLimit = function(section) {
            var tab = vm.tabs.filter(function(tab) {
                return tab.label == section;
            })[0];

            return vm.overview.Selections[tab.category].Limit;
        };

        vm.save = function() {
            vm.isSaving = true;
            vm.store.save();
        };
    }
});