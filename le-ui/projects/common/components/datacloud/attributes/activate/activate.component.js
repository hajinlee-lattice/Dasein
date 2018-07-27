/* jshint -W014 */
angular.module('common.attributes.activate', [])
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
                    var category = $stateParams.category;

                    AttrConfigStore.set('category', category);
                    
                    AttrConfigService.getConfig('activation', category).then(function(response) {
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
    controller: function (AttrConfigStore, $timeout, Banner, Modal, Notice) {
        var vm = this;

        vm.store = AttrConfigStore;
        vm.filters = vm.store.get('filters');

        vm.$onInit = function() {
            // Notice.success({
            //     title: '500 Internal Server ', 
            //     message: '<i>Generic rest call failure (LEDP_00002)</i>'
            // }, callbackFn);

            // Banner.warning({
            //     title: 'Warning: Lorem Ipsum Incoming', 
            //     message: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.'
            // });

            // Notice.success({
            //     title: 'Operation Succeeded', 
            //     message: 'The thing that you wanted done is now done, jackass.'
            // });

            // Banner.info({
            //     title: 'Deprecated Attributes Detected', 
            //     message: "You can't take any action on these attributes.  It is advised to disable them from your workflows as they are no longer supported"
            // });
        };
    }
});