import './connectors-list.component';
import './profiles.component';
import ConnectorsRoutes from "./react-routing";
angular
.module('le.connectors', ['le.connectors.list', 'le.connectors.profile'
])
.config(function($stateProvider, $urlRouterProvider) {
    $stateProvider

        .state('home.connectors', {
            url: '/connectors',
            onEnter: function(){
            },
            onExit: () => {
                ConnectorsRoutes.clean();
            },
            params: {
                tenantName: { dynamic: true, value: '' },
                pageIcon: 'ico-cog',
                pageTitle: 'Connect Applications'
            },
            
            views: {
                'summary@': {
                    component: 'connectorListComponent'
                },
                'main@': {
                    component: 'profilesContainerComponent'
                }
            }
        });
        
});