import './connectors-list.component';
angular
.module('le.connectors', ['le.connectors.list'
])
.config(function($stateProvider) {
    $stateProvider


        .state('home.connectors', {
            url: '/connectors',
            onEnter: function(){
            },
            params: {
                tenantName: { dynamic: true, value: '' },
                pageIcon: 'ico-cog',
                pageTitle: 'Connect Applications'
            },
            views: {
                'summary@': {
                    component: 'connectorListComponent'
                }
            },
            // redirectTo: 'home.import.entry.accounts'
        });
});