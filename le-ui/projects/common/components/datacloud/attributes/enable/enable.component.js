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
                    value: ''
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
                config: ['$q', '$state', '$stateParams', 'AttrConfigService', 'AttrConfigStore', 'overview', 'DataCloudStore', function($q, $state, $stateParams, AttrConfigService, AttrConfigStore, overview, DataCloudStore) {
                    var deferred = $q.defer();
                    var section = $stateParams.section;
                    var category = $stateParams.category;

                    var tab = overview.Selections.filter(function(item) {
                        return item.DisplayName == section;
                    });

                    var categories = tab[0].Categories;

                    if (!category) {
                        DataCloudStore.topCategories.some(function(key) {
                            if (key != 'Lattice Ratings' && categories[key] > 0) {
                                return category = key;
                            }
                        });
                    }

                    AttrConfigStore.set('category', category);
                    AttrConfigStore.set('categories', categories);
                    
                    AttrConfigService.getConfig('usage', category, { 
                        usage: section 
                    }).then(function(response) {
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
    controller: function(AttrConfigStore) {
        var vm = this;

        vm.store = AttrConfigStore;
        vm.filters = vm.store.get('filters');
        vm.uiCanExit = vm.store.uiCanExit;

        vm.$onInit = function() {
            vm.categories = vm.store.get('categories');
        };
    }
});