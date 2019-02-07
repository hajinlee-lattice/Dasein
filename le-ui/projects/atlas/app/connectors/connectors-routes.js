import './connectors-list.component';
import './systems.component';
import './profiles.component';
import ConnectorsRoutes from "./connectors-routing";
angular
.module('le.connectors', ['le.connectors.list', 'le.connectors.profile','le.systems.list'
])
.config(function($stateProvider) {
    $stateProvider

        .state('home.connectors', {
            url: '/connectors',
            onEnter: function(){
            },
            onExit: () => {
                ConnectorsRoutes.clearRouter();
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
                    component: 'systemsComponent'
                }
            }
        });
        
});