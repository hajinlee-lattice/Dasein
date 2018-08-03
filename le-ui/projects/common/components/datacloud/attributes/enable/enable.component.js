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
                config: ['$q', '$stateParams', 'AttrConfigService', 'AttrConfigStore', 'overview', function($q, $stateParams, AttrConfigService, AttrConfigStore, overview) {
                    var deferred = $q.defer();
                    var section = $stateParams.section;
                    var category = $stateParams.category;

                    if (!category) {
                        Object.keys(overview.AttrNums).some(function(key) {
                            if (key != 'Lattice Ratings' && overview.AttrNums[key] > 0) {
                                return category = key;
                            }
                        });

                        $stateParams.category = category;
                    }

                    AttrConfigStore.set('category', category);
                    
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
    controller: function($q, AttrConfigStore, Modal) {
        var vm = this;

        vm.store = AttrConfigStore;
        vm.filters = vm.store.get('filters');

        vm.$onInit = function() {
            vm.categories = vm.overview.AttrNums;
        };
        
        vm.uiCanExit = vm.store.uiCanExit;
    }
});