
angular.module('common.attributes')
.service('AttrConfigStore', function($q, $state, AttrConfigService) {
    var store = this;

    this.init = function(){
        this.data = {
            activate: {
                attributes: [],
                overview: []
            },
            enable: {
                attributes: [],
                overview: []
            },
            edit: {
                attributes: [],
                overview: []
            }
        };

        this.tabs = {
            activate: [{
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

    this.getSection = function() {
        var map = {
            'home.attributes.activate': 'activate',
            'home.attributes.enable': 'enable',
            'home.attributes.edit': 'edit'
        };

        return map[$state.current.name];
    };

    this.getTabMetadata = function(section) {
        return this.tabs[section];
    };

    this.putConfig = function(type, category, usage, data) {
        var deferred = $q.defer();
        
        AttrConfigService.putConfig(type, category, usage, data).then(function(data) {
            deferred.resolve(data);
        });

        return deferred.promise;
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