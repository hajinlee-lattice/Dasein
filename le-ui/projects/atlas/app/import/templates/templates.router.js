import './templates';
import './components/summary';
angular
.module('lp.importtemplates', [
    
    'le.import.templates',
    'le.summary'
])
.config(function($stateProvider) {
    $stateProvider

        .state('home.importtemplates', {
            url: '/templates',
            onEnter: function(){
                console.log('ENTERED');
            },
            params: {
                tenantName: { dynamic: true, value: '' },
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            views: {
                'summary@': {
                    component: 'leSummaryComponent'
                },
                'main@': {
                    component: 'templatesComponent'
                }
            },
            // redirectTo: 'home.import.entry.accounts'
        });
});