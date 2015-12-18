angular
    .module('pd.builder.category', [
    ])
    .service('CategoryModel', function($rootScope, $q, _, CategoryService) {
        var CategoryModel = this;

        this.MasterList = {}

        this.getList = function(args) {
            var deferred = $q.defer();

            var list = this.MasterList[args.AttrKey];

            if (list && args.ParentValue) {
                list = list[args.ParentValue];
            }
            
            if (list) {
                setTimeout(function() {
                    deferred.resolve(list);
                },1);
            } else {
                CategoryService.get(args).then(function(list) {
                    var list = list || [];

                    // FIXME - Fudging the numbers a bit for the demo...
                    list.forEach(function(item, index) {
                        item.total = Math.round(Math.random() * 20);
                        item.lift = (Math.random() * 3.0).toFixed(1) + 'x';

                        item.revenue = Math.round(Math.random() * 30000000);
                        
                        item.lattice_companies = item.Properties.CompanyCount;
                        item.their_companies = Math.round(Math.random() * item.lattice_companies);
                        item.customers = Math.round(Math.random() * item.their_companies);

                        item.their_companies = item.their_companies > item.lattice_companies ? item.lattice_companies >> 1 : item.their_companies;
                        item.customers = item.customers > item.their_companies ? item.their_companies >> 1 : item.customers

                        item.mediump = ((item.their_companies / item.lattice_companies) * 100).toFixed(3);
                        item.smallp = ((item.customers / item.lattice_companies) * 100).toFixed(3);
                        item.smallp_of_theirs = ((item.customers / item.their_companies) * 100).toFixed(3);

                        item.mediump = item.mediump < 16 ? 16 : item.mediump;
                        item.smallp = item.smallp < 10 ? 10 : item.smallp;

                        item.selected = false;
                    });

                    deferred.resolve(list);

                    if (CategoryModel.MasterList[args.AttrKey] && args.ParentValue) {
                        CategoryModel.MasterList[args.AttrKey][args.ParentValue] = list;
                    } else {
                        CategoryModel.MasterList[args.AttrKey] = list;
                    }
                    console.log('builder category list:', list);
                });
            }

            return deferred.promise;
        }
    })
    .service('CategoryService', function($http, $q, _) {
        this.QueryMap = {
            Industry: [ 'Industry' ],
            Locations: [ 'Region', 'State', 'Country' ]
        };

        this.get = function(args) {
            var deferred = $q.defer();
            var queries = [];
            var components = this.QueryMap[args.AttrKey];

            (components || [ args.AttrKey ]).forEach(function(AttrKey, index) {
                query = {
                    AttrKey: AttrKey
                };

                if (args.ParentKey) {
                    query.ParentKey = args.ParentKey;
                    query.ParentValue = args.ParentValue;
                }
                
                queries.push(query);
            });
            

            var url = '/pls/amattributes?populate=true&queries=' + encodeURIComponent(JSON.stringify(queries));

            $http({
                method: 'GET',
                url: url
            }).then(
                function onSuccess(response) {
                    var result = [];

                    for (var i=0; i<response.data.length; i++) {
                        result = result.concat(response.data[i]);
                    }

                    console.log(result, response);

                    deferred.resolve(result);
                }, function onError(response) {
                    deferred.reject('Error fetching data.')
                }
            );
            
            return deferred.promise;
        };
    })
    .controller('CategoryCtrl', function($scope, $rootScope, $state, $stateParams, CategoryModel) {
        $scope.lists = [];
        $scope.AttrKey = AttrKey = $stateParams.AttrKey;
        $scope.ParentKey = $stateParams.ParentKey;
        $scope.ParentValue = $stateParams.ParentValue;
        $scope.truncate_limit = 28;

        if (!AttrKey) {
            return console.log('<!> No stateParams provided.');
        }

        console.log($stateParams, $scope);

        CategoryModel.getList($stateParams).then(function(list) {
            var chunks = 3;

            $scope.total = list.length;

            /*
            // Break the array up into rows of 3 each
            var list = _.chain(list).groupBy(function(element, index) {
                return Math.floor(index / chunks);
            }).toArray().value();
            */

            $scope.list = list;
        });

        $scope.handleTileSelection = function() {
            var Industry = CategoryModel.MasterList.Industry ||  [];
            var Locations = CategoryModel.MasterList.Locations ||  [];
            var EmployeesRange = CategoryModel.MasterList.EmployeesRange ||  [];
            var list = Industry.concat(Locations,EmployeesRange);

            var SelectedList = list.filter(function(item) {
                return item.selected;
            });

            $rootScope.$broadcast('Builder-Sidebar-List', SelectedList);
        }
    }
);