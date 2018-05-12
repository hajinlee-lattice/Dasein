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
            resolve: {
                overview: ['$q', 'AttrConfigService', function($q, AttrConfigService) {
                    var deferred = $q.defer();
                    
                    AttrConfigService.getOverview('activation').then(function(response) {
                        console.log('resolve getOverview', response);
                        deferred.resolve(response.data || []);
                    });

                    return deferred.promise;
                }],
                config: ['$q', '$stateParams', 'AttrConfigService', function($q, $stateParams, AttrConfigService) {
                    var deferred = $q.defer();
                    
                    AttrConfigService.getConfig('activation', $stateParams.category).then(function(response) {
                        console.log('resolve getSubCategories', response);
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
    controller: function ($state, $stateParams) {
        var vm = this;

        vm.options = {
            section: 'activate'
        };

        vm.$onInit = function() {
            console.log('init attrActivate', vm);
        };
    }
});