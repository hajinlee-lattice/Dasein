angular
    .module('pd.builder.attributes', [])
    .service('AttributesModel', function($q, AttributesService, $filter) {
        var AttributesModel = this;

        this.SubCategoryMap = {
            'Industry': 'SubIndustry',
            'Locations': 'State'
        };

        this.ParentCategoryMap = {
            'Locations': 'Region',
            'Employees Range': 'EmployeesRange',
            'Revenue Range': 'RevenueRange'
        };

        this.MasterList = [];

        this.getMaster = function(args) {
            var fields = {
                AttrKey: (this.ParentCategoryMap[args.AttrKey] || args.AttrKey)
            };

            if (args.AttrValue) fields.AttrValue = args.AttrValue;
            if (args.ParentValue) fields.ParentValue = args.ParentValue;
            if (args.ParentKey) fields.ParentKey = args.ParentKey;

            var list = $filter('filter')(this.MasterList, fields, true);
            console.log('getMaster', fields, list, this.MasterList);

            return list || [];
        }

        this.getList = function(args) {
            var deferred = $q.defer(),
                master = this.getMaster(args);

            if (master && master.length > 0) {
                setTimeout(function() {
                    deferred.resolve(master);
                }, 1);
            } else {
                master = this.MasterList;

                AttributesService.get(args).then(function(list) {
                    list = list || [];

                    // FIXME - Fudging the numbers a bit for the demo...
                    list.forEach(function(item, index) {
                        // FIXME - Make REGION top level
                        if (item.ParentKey == 'Country')
                            item.ParentKey = '_OBJECT_';

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

                        item.mediump = item.mediump < 15 ? 15 : item.mediump;
                        item.smallp = item.smallp < 10 ? 10 : item.smallp;

                        item.selected = false;

                        master.push(item);
                    });

                    deferred.resolve(list);

                    console.log('builder category list:', list);
                });
            }

            return deferred.promise;
        }
    })
    .service('AttributesService', function($http, $q) {
        this.QueryMap = {
            'Industry': [ 'Industry' ],
            'Locations': [ 'Region' ],
            'Employees Range': [ 'EmployeesRange' ],
            'Revenue Range': [ 'RevenueRange' ]
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
            
            var url = '/pls/amattributes' +
                      '?populate=true' +
                      '&queries=' + encodeURIComponent(JSON.stringify(queries));

            $http({
                method: 'GET',
                url: url
            }).then(
                function onSuccess(response) {
                    var result = [];

                    for (var i=0; i<response.data.length; i++) {
                        result = result.concat(response.data[i]);
                    }

                    deferred.resolve(result);
                }, function onError(response) {
                    deferred.reject('Error fetching data.')
                }
            );
            
            return deferred.promise;
        };
    })
    .controller('AttributesCtrl', function($state, $stateParams, AttributesModel) {
        this.SubCategoryMap = AttributesModel.SubCategoryMap;
        this.ParentCategoryMap = AttributesModel.ParentCategoryMap;

        angular.extend(this, $stateParams, {
            init: function() {
                this.truncate_limit = 32;

                if (!this.AttrKey) {
                    return console.log('<!> No stateParams provided');
                }

                AttributesModel
                    .getList($stateParams)
                    .then(angular.bind(this, this.setList));
            },
            setList: function(list) {
                this.total = list.length;
                this.list = list;
                this.SubCategory = this.SubCategoryMap[this.AttrKey];
                this.ParentCategory = this.ParentCategoryMap[this.AttrKey] || this.AttrKey;
            },
            handleTileClick: function($event, item) {
                if (this.SubCategory) {
                    $state.go("builder.category", { 
                        ParentKey: this.ParentCategory, 
                        ParentValue: item.AttrValue, 
                        AttrKey: this.SubCategory
                    });
                }
            },
            handleTileSelect: function(selected) {
                var SubCategory = this.SubCategory;

                selected.modified = Date.now();
                selected.visible = !SubCategory;

                if (SubCategory) {
                    AttributesModel
                        .getList({
                            ParentKey: this.ParentCategory,
                            ParentValue: selected.AttrValue,
                            AttrKey: SubCategory
                        })
                        .then(angular.bind(this, function(result) {
                            selected.total = result.length;

                            result.forEach(function(item, index) {
                                item.selected = selected.selected;
                                item.modified = Date.now();
                                item.visible = true;
                            });
                        }));
                } else {
                    AttributesModel
                        .getList({
                            AttrKey: selected.ParentKey,
                            AttrValue: selected.ParentValue
                        })
                        .then(angular.bind(this, function(result) {
                            result.forEach(function(item, index) {
                                item.selected = true;
                            });
                        }));
                }
            }
        });

        this.init();
    });