angular
    .module('common.attributes.activate', ['mainApp.core.redux'])
    .config(function ($stateProvider) {
        $stateProvider.state('home.attributes.activate', {
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
            onExit: [
                'AttrConfigStore',
                function (AttrConfigStore) {
                    AttrConfigStore.init();
                }
            ],
            resolve: {
                overview: [
                    '$q',
                    'AttrConfigService',
                    function ($q, AttrConfigService) {
                        var deferred = $q.defer();

                        AttrConfigService.getOverview('activation').then(
                            function (response) {
                                deferred.resolve(response.data || []);
                            }
                        );

                        return deferred.promise;
                    }
                ],
                config: [
                    '$q',
                    '$stateParams',
                    'AttrConfigService',
                    'AttrConfigStore',
                    function (
                        $q,
                        $stateParams,
                        AttrConfigService,
                        AttrConfigStore
                    ) {
                        var deferred = $q.defer();
                        var category = $stateParams.category;

                        AttrConfigStore.set('category', category);

                        AttrConfigService.getConfig(
                            'activation',
                            category
                        ).then(function (response) {
                            AttrConfigStore.setData(
                                'config',
                                response.data || []
                            );
                            deferred.resolve(response.data || []);
                        });

                        return deferred.promise;
                    }
                ]
            },
            views: {
                'subsummary@': 'attrSubheader',
                'main@': 'attrActivate'
            }
        });
    })
    .component('attrActivate', {
        templateUrl:
            '/components/datacloud/attributes/activate/activate.component.html',
        bindings: {
            overview: '<',
            config: '<'
        },
        controller: function (AttrConfigStore, $state, $ngRedux) {
            let vm = this;

            vm.store = AttrConfigStore;
            vm.filters = vm.store.get('filters');
            vm.uiCanExit = vm.store.uiCanExit;

            vm.redux = $state.get('home.attributes').data.redux;

            // console.log(
            //     '-!- Redux controller init',
            //     this,
            //     $state.get('home.attributes')
            // );

            vm.$onInit = function () {
                // $ngRedux.subscribe(state => {
                //     console.log('-!- Redux store has changed', vm.redux.store);
                // });

                // vm.redux.get();
            };
        }
    });
