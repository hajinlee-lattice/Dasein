
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

        this.data = {
            original: {},
            config: {},
            overview: {}
        };

        this.tabs = {
            activate: [{
                    label: "Intent Attributes",
                    category: "Intent",
                    supplemental: 'ACTIVATED'
                },{
                    label: "Technology Profile Attributes",
                    category: "Technology Profile",
                    supplemental: 'ACTIVATED'
                },{
                    label: "My Account Attributes",
                    category: "My Attributes",
                    supplemental: 'ACTIVATED'
                },{
                    label: "My Contact Attributes",
                    category: "Contact Attributes",
                    supplemental: 'ACTIVATED'
                }
            ],
            enable: [{
                    label: "Segmentation",
                    category: "Segment",
                    supplemental: 'ENABLED'
                },{
                    label: "Export",
                    category: "Enrichment",
                    supplemental: 'ENABLED'
                },{
                    label: "Talking Points",
                    category: "TalkingPoint",
                    supplemental: 'ENABLED'
                },{
                    label: "Company Profile",
                    category: "CompanyProfile",
                    supplemental: 'ENABLED'
                }
            ],
            edit: [{
                    label: "Intent Attributes",
                    category: "Intent",
                    supplemesntal: 'ACTIVATED'
                },{
                    label: "Technology Profile Attributes",
                    category: "Technology Profile",
                    supplemental: 'ACTIVATED'
                },{
                    label: "My Attributes",
                    category: "My Attributes",
                    supplemental: 'ACTIVATED'
                },{
                    label: "Contact Attributes",
                    category: "Contact Attributes",
                    supplemental: 'ACTIVATED'
                }
            ]
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

    this.getTabMetadata = function(section) {
        return this.tabs[section];
    };

    this.getSelected = function() {
        return this.selected;
    };

    this.setSelected = function(total) {
        this.selected = total;
    };

    this.getLimit = function() {
        return this.limit;
    };

    this.setLimit = function(total) {
        this.limit = total;
    };

    this.getUsageLimit = function(overview, area) {
        var section = this.getSection();
        var tabs = this.getTabMetadata(section);
        var tab = tabs.filter(function(tab) {
            return tab.label == area;
        })[0];

        return overview.Selections[tab.category].Limit;
    };

    this.putConfig = function(type, category, usage, data) {
        var deferred = $q.defer();
        
        AttrConfigService.putConfig(type, category, usage, data).then(function(data) {
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
            Select: []
        };

        var original = this.getData('original').original;

        if (!activate) {
            usage.usage = $stateParams.section;
            data.Deselect = [];
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
                } else if (!activate) {
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