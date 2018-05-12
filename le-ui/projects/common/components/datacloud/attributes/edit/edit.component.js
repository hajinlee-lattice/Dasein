angular.module('common.attributes.edit', [
    'common.attributes.subheader'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.attributes.edit', {
            url: '/edit/:category',
            params: {
                category: {
                    dynamic: true,
                    value: 'My Account'
                }
            },
            resolve: {
                tabs: [function() {
                    return [{
                        label: "My Account Attributes",
                        category: "My Account",
                        total: 500
                    },{
                        label: "My Contact Attributes",
                        category: "My Contact",
                        total: 500
                    }];
                }]
            },
            views: {
                "subsummary@": "attrSubheader",
                "main@": "attrEdit"
            }
        });
})
.component('attrEdit', {
    templateUrl: '/components/datacloud/attributes/edit/edit.component.html',
    controller: function ($stateParams) {
        this.$onInit = function() {
            this.params = $stateParams;
        };
    }
});