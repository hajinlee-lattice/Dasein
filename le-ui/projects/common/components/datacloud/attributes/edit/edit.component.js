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
            resolve: {},
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