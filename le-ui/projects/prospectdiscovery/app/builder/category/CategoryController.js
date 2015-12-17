angular
    .module('pd.builder.category', [

    ])
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
    .controller('CategoryCtrl', function($scope, $rootScope, $stateParams, CategoryService) {
        $scope.lists = [];
        $scope.AttrKey = AttrKey = $stateParams.AttrKey;
        $scope.ParentKey = $stateParams.ParentKey;
        $scope.ParentValue = $stateParams.ParentValue;

        if (!AttrKey) {
            return console.log('<!> No stateParams provided.');
        }

        console.log($stateParams, $scope);
        // will fetch/format all attributes for given category
        CategoryService.get($stateParams).then(function(data) {
            var data = data || [],
                chunks = 3,
                truncate_limit = 24, // truncate with ellipsis
                total = $scope.total = data.length;

            // FIXME - Fudging the numbers a bit for the demo...
            data.forEach(function(item, index) {
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
            });

            // Break the array up into rows of 3 each
            data = _.chain(data).groupBy(function(element, index) {
                return Math.floor(index / chunks);
            }).toArray().value();

            $scope.lists = data;
            $scope.truncate_limit = truncate_limit;

            console.log('builder category list:', $scope.lists);
        });
    }
);