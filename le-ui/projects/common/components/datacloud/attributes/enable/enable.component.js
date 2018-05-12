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
            resolve: {
                overview: ['$q', 'AttrConfigService', function($q, AttrConfigService) {
                    var deferred = $q.defer();
                    
                    AttrConfigService.getOverview('usage').then(function(response) {
                        console.log('resolve usage getOverview', response);
                        deferred.resolve(response.data || []);
                    });

                    return deferred.promise;
                }],
                config: ['$q', '$stateParams', 'AttrConfigService', function($q, $stateParams, AttrConfigService) {
                    var deferred = $q.defer();
                    var section = $stateParams.section;
                    
                    AttrConfigService.getConfig('usage', $stateParams.category, { usage: section }).then(function(response) {
                        console.log('resolve usage getSubCategories', response);
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
    controller: function($stateParams) {
        var vm = this;

        vm.options = {
            section: 'enable'
        };

        vm.$onInit = function() {
            console.log('init attrActivate', vm);
        };
    }
});