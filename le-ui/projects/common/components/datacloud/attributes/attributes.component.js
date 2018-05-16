angular.module('common.attributes', [
    'common.attributes.header',
    'common.attributes.filters',
    'common.attributes.categories',
    'common.attributes.list',
    'common.attributes.enable',
    'common.attributes.activate',
    'common.attributes.edit'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.attributes', {
            url: '/attributes',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Attribute Admin'
            },
            resolve: {
                tabs: [function() {
                    return [{
                        sref: "home.attributes.activate",
                        label: "Activate Premium Attributes"
                    },{
                        sref: "home.attributes.enable",
                        label: "Enable & Disable Attributes"
                    // },{
                    //     sref: "home.attributes.edit",
                    //     label: "Edit Name & Description"
                    }];
                }]
            },
            views: {
                "summary@": "attrHeader"
            },
            redirectTo: 'home.attributes.activate'
        });
})
.component('adminAttributes', {
    templateUrl: '',
    controller: function() {}
});