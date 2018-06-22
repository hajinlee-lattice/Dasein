
angular.module('common.attributes')
.service('AttrConfigStore', function($q, $state, $stateParams, $timeout, AttrConfigService) {
    var store = this;

    this.init = function(){
        this.filters = {
            page: 1,
            pagesize: 25,
            sortPrefix: '+',
            queryText: '',
            showFilterBy: false,
            show: { 
                Selected: false,
                IsPremium: false
            },
            hide: { 
                Selected: false,
                IsPremium: false 
            }
        };

        this.limit = -1;
        this.selected = [];
        this.start_selected = [];
        this.category = '';

        this.data = {
            original: {},
            config: {},
            overview: {},
            buckets: {}
        };
    };

    this.getData = function() {
        return this.data;
    };

    this.setData = function(type, data) {
        this.data[type] = data;
    };

    this.getSection = function() {
        var map = {
            'home.attributes.activate': 'activate',
            'home.attributes.enable': 'enable',
            'home.attributes.edit': 'edit'
        };

        return map[$state.current.name];
    };

    this.getFilters = function() {
        return this.filters;
    };

    this.getActiveTabData = function() {
        var page = this.getSection();
        var param = page == 'activate' ? 'category' : 'section';
        var active = $stateParams[param];
        var data = this.getData().overview;
        var tab = [];

        if (data && data.Selections) {
            tab = data.Selections.filter(function(tab) {
                return tab.DisplayName == active;
            });
        }

        return tab[0] || {};
    };

    this.getSelectedTotal = function() {
        var selected = this.selected;
        var section = this.getSection();
        var total = selected.length;
        var tab, started;

        if (section == 'enable') {
            started = this.getStartSelected();
            tab = this.getActiveTabData();

            if (tab.Selected) {
                total = tab.Selected + (total - started.length);
            }
        }

        //console.log('getSelectedTotal', section, total, tab, started);
        return total;
    };

    this.getSelected = function() {
        return this.selected;
    };

    this.setSelected = function(total) {
        this.selected = total;
    };

    this.getStartSelected = function() {
        return this.start_selected;
    };

    this.setStartSelected = function(total) {
        this.start_selected = total;
    };

    this.getLimit = function() {
        return this.limit;
    };

    this.setLimit = function(total) {
        this.limit = total;
    };

    this.getCategory = function() {
        return this.category;
    };

    this.setCategory = function(category) {
        this.category = category;
    };

    this.getUsageLimit = function(overview, area) {
        var section = this.getSection();
        var tab = overview.Selections.filter(function(tab) {
            return tab.DisplayName == area;
        })[0];

        return tab.Limit;
    };

    this.putConfig = function(type, category, usage, data) {
        var deferred = $q.defer();
        
        AttrConfigService.putConfig(type, category, usage, data).then(function(data) {
            deferred.resolve(data);
        });

        return deferred.promise;
    };

    this.readBucketData = function(category) {
        return this.data.buckets;
    };

    this.getBucketData = function(category, subcategory) {
        var deferred = $q.defer();
        
        AttrConfigService.getBucketData(category, subcategory).then(function(data) {
            store.data.buckets[subcategory] = data.data;
            deferred.resolve(data);
        });

        return deferred.promise;
    };

    this.isChanged = function() {
        if (!this.data.original.Entity) {
            return true;
        }

        var hasChanged = false;
        var subcategories = this.data.original.Subcategories;

        this.data.config.Subcategories.forEach(function(subcategory, index) {
            subcategory.Attributes.forEach(function(attribute, i) {
                if (subcategories[index].Attributes[i].Selected !== attribute.Selected) {
                    hasChanged = true;
                }
            });
        });

        return hasChanged;
    };

    this.saveConfig = function() {
        var activate = this.getSection() == 'activate';
        var type = activate ? 'activation' : 'usage';
        var category = $stateParams.category;
        var usage = {};
        var data = {
            Select: [],
            Deselect: []
        };

        var original = this.getData('original').original;

        if (!activate) {
            usage.usage = $stateParams.section;
        }

        this.data.config.Subcategories.forEach(function(item, index) {
            var oSub = original.Subcategories[index];

            item.Attributes.forEach(function(attr, i) {
                var oAttr = oSub.Attributes[i];
                
                if (oAttr.Selected === attr.Selected) {
                    return;
                }

                if (attr.Selected) {
                    data.Select.push(attr.Attribute);
                } else {
                    data.Deselect.push(attr.Attribute);
                }
            });
        });
        
        
        this.putConfig(type, category, usage, data).then(function() {
            store.setData('original', JSON.parse(JSON.stringify(store.data.config)));

            $timeout(function() {
                $state.reload();
            }, 500);
        });
    };

    this.init();
}) 
.service('AttrConfigService', function($q, $http) {
    this.getOverview = function(section) {
        var deferred = $q.defer();
        
        $http({
            method: 'GET',
            url: '/pls/attrconfig/' + section + '/overview'
        }).then(function(response){
            deferred.resolve(response);
        });
        
        return deferred.promise;
    };

    this.getConfig = function(section, category, params) {
        var deferred = $q.defer();
        
        $http({
            method: 'GET',
            url: '/pls/attrconfig/' + section + '/config/category/' + category,
            params: params
        }).then(function(response) {
            deferred.resolve(response);
        });
        
        return deferred.promise;
    };

    this.getBucketData = function(category, subcategory) {
        var deferred = $q.defer();
        
        $http({
            method: 'GET',
            url: '/pls/attrconfig/stats/category/' + category,
            params: {
                'subcategory': subcategory
            } 
        }).then(function(response) {
            deferred.resolve(response);
        });
        
        return deferred.promise;
    };

    this.putConfig = function(section, category, params, data) {
        var deferred = $q.defer();
        
        $http({
            method: 'PUT',
            url: '/pls/attrconfig/' + section + '/config/category/' + category,
            params: params,
            data: data
        }).then(function(response) {
            deferred.resolve(response);
        });
        
        return deferred.promise;
    };
});