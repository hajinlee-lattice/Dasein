angular.module('common.attributes.edit', [
    'common.attributes.edit.filters',
    'common.attributes.edit.list'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.attributes.edit', {
            url: '/edit/:category',
            params: {
                category: {
                    dynamic: false,
                    value: 'My Attributes'
                }
            },
            onExit: ['AttrConfigStore', function(AttrConfigStore) {
                AttrConfigStore.init();
            }],
            resolve: {
                overview: ['$q', 'AttrConfigService', function($q, AttrConfigService) {
                    var deferred = $q.defer();
                    
                    AttrConfigService.getOverview('name').then(function(response) {
                        deferred.resolve(response.data || []);
                    });

                    return deferred.promise;
                }],
                config: ['$q', '$stateParams', 'AttrConfigService', 'AttrConfigStore', function($q, $stateParams, AttrConfigService, AttrConfigStore) {
                    var deferred = $q.defer();
                    var category = $stateParams.category;

                    AttrConfigStore.set('category', category);
                    
                    AttrConfigService.getConfig('name', category).then(function(response) {
                        AttrConfigStore.setData('config', response.data || []);
                        deferred.resolve(response.data || []);
                    });

                    return deferred.promise;
                }]
            },
            views: {
                "subsummary@": "attrSubheader",
                "main@": "attrEdit"
            }
        });
})
.component('attrEdit', {
    templateUrl: '/components/datacloud/attributes/edit/edit.component.html',
    bindings: {
        overview: '<',
        config: '<'
    },
    controller: function ($stateParams, AttrConfigStore) {
        var vm = this;

        vm.store = AttrConfigStore;
        vm.filters = vm.store.get('filters');
        vm.uiCanExit = vm.store.uiCanExit;
    }
});