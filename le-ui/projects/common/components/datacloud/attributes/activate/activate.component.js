/* jshint -W014 */
angular.module('common.attributes.activate', [
    'common.attributes.subheader'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.attributes.activate', {
            url: '/activate/:category/:subcategory',
            params: {
                category: {
                    dynamic: false,
                    value: 'Intent'
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
                    
                    AttrConfigService.getOverview('activation').then(function(response) {
                        deferred.resolve(response.data || []);
                    });

                    return deferred.promise;
                }],
                config: ['$q', '$stateParams', 'AttrConfigService', 'AttrConfigStore', function($q, $stateParams, AttrConfigService, AttrConfigStore) {
                    var deferred = $q.defer();
                    
                    AttrConfigService.getConfig('activation', $stateParams.category).then(function(response) {
                        AttrConfigStore.setData('config', response.data || []);
                        deferred.resolve(response.data || []);
                    });

                    return deferred.promise;
                }]
            },
            views: {
                "subsummary@": "attrSubheader",
                "main@": "attrActivate"
            }
        });
})
.component('attrActivate', {
    templateUrl: '/components/datacloud/attributes/activate/activate.component.html',
    bindings: {
        overview: '<',
        config: '<'
    },
    controller: function ($state, $stateParams, AttrConfigStore) {
        var vm = this;

        vm.filters = AttrConfigStore.getFilters();
        vm.store = AttrConfigStore;
        vm.isSaving = false;

        vm.$onInit = function() {
            console.log('init attrActivate', vm);

            vm.data = vm.store.getData();
            vm.section = vm.store.getSection();
            vm.params = $stateParams;
            vm.categories = vm.overview.AttrNums;
            
            vm.store.setLimit(vm.overview[vm.params.category].Limit);
        };

        vm.save = function() {
            vm.isSaving = true;
            vm.store.save();
        };
    }
});