import 'atlas/react/react-angular-main.component';
import { actions, reducer } from './connections.redux';
angular
.module('le.connectors', ['lp.sfdc', 'common.utilities.browserstorage', 'common.services.featureflag', 'common.modal'])
.service('ConnectorsService', function ($state, BrowserStorageUtility, FeatureFlagService, SfdcService, Notice) {
        let ConnectorsService = this;
        this.getConnector = function () {
            console.log('Test', $state.router.locationConfig.$location.$$hash);
            let hash = $state.router.locationConfig.$location.$$hash;
            console.log(hash);
            let selected = '';
            if (hash != '') {
                let hashArray = hash.split('/');
                // console.log('ARRAY ', hashArray[1]);
                selected = hashArray[1];
            }
            return selected;
        };
        this.generateAuthToken = function () {
            let clientSession = BrowserStorageUtility.getClientSession();
            let emailAddress = clientSession.EmailAddress;
            let tenantId = clientSession.Tenant.Identifier;
            SfdcService.generateAuthToken(emailAddress, tenantId).then(function (result) {
                if (result.Success == true) {
                    Notice.success({
                        delay: 5000,
                        title: 'Email sent to ' + emailAddress,
                        message: 'Your one-time authentication token has been sent to your email.'
                    });
                } else {
                    Banner.error({ message: 'Failed to Generate Salesforce Access Token.' });

                }
            });
        }
        this.isMarketoEnabled = function () {
            let connectorsEnabled = FeatureFlagService.FlagIsEnabled(FeatureFlagService.Flags().ENABLE_EXTERNAL_INTEGRATION);
            actions.setMarketoEnabled(connectorsEnabled);
            return connectorsEnabled;
        }
    })
.config(function($stateProvider) {
    $stateProvider

        .state('home.connectors', {
            url: '/connectors',
            params: {
                tenantName: { dynamic: true, value: '' },
                pageIcon: 'ico-connectors',
                pageTitle: 'Connections'
            },
            resolve: {
                path: () => {
                    return 'connectorslist';
                },
                ngservices: (ConnectorsService) => {
                    let obj = {
                        ConnectorsService: ConnectorsService
                    }
                    return obj;
                }
            },
            views: {
                'main@': {
                    component: 'reactAngularMainComponent'
                }
            }
        });
        
});