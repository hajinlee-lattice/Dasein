angular
.module('lp.template', [
    'lp.template.leaf',
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.template', {
            url: '/template',
        })
        .state('home.template.leaf', {
            url: '/leaf',
            params: {
                pageIcon: '',
                pageTitle: "Template's Leaf"
            },
            views: {
                "main@": {
                    controller: 'TemplateController',
                    controllerAs: 'vm',
                    templateUrl: 'app/template/content/leaf/leaf.component.html'
                }
            }
        })
});