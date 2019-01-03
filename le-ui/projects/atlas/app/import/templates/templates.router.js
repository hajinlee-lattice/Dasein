import './templates';
import './components/summary';
angular
.module('lp.importtemplates', [
    
    'le.import.templates',
    'le.summary',
    'lp.import'
])
.config(function($stateProvider) {
    $stateProvider


        .state('home.importtemplates', {
            url: '/templates',
            onEnter: function(ImportWizardStore){
                ImportWizardStore.clear();
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