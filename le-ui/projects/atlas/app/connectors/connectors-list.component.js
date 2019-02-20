
import React, { Component, react2angular } from "common/react-vendor";
import './connectors-list.scss';
import LeVPanel from 'common/widgets/container/le-v-panel';
import LeHPanel from 'common/widgets/container/le-h-panel';
import { LeToolBar, HORIZONTAL } from 'common/widgets/toolbar/le-toolbar';
import LeButton from "common/widgets/buttons/le-button";
import { CENTER, LEFT } from 'common/widgets/container/le-alignments';
import Connector from './connector.component';
import ConnectorsRoutes from "./connectors-routing";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

import ConnectorService, { MARKETO, SALESFORCE, ELOQUA } from './connectors.service';

export class ConnectorList extends Component {
    constructor(props) {
        super(props);
        // console.log('THE PROPS ', props);
        this.clickHandler = this.clickHandler.bind(this);
        this.generateAuthTokenClickHandler = this.generateAuthTokenClickHandler.bind(this);
        this.state = {
            connectorSelected: this.props.ConnectorsService.getConnector(),
            userValidated: false,
            userInfo: null
        };
        this.connectors = ConnectorService.getList(this.props.ConnectorsService.isMarketoEnabled());

    }
    componentDidMount() {
        this.router = ConnectorsRoutes.getRouter();
        if (name != MARKETO) {
            ConnectorService.setUserValidated(true);
        } else {
            this.validateUser();
        }

        this.router.stateService.go('profiles', { nameConnector: this.state.connectorSelected });
        ConnectorService.getList();

    }

    generateAuthTokenClickHandler() {
        if (ConnectorService.getConnectorName() != '' && ConnectorService.getConnectorName() != MARKETO) {
            ConnectorService.sendMSG(() => {
                this.props.ConnectorsService.generateAuthToken();
            });
        }

    }

    clickHandler(name) {
        // console.log('SELECTED ', name);
        this.setState({ connectorSelected: name });
        // let nameConnector = name;
        ConnectorService.setConnectorName(name);
        if (name != MARKETO) {
            ConnectorService.setUserValidated(true);
        } else {
            ConnectorService.setUserValidated(true);
            this.validateUser();
        }
        this.router.stateService.go('profilesconnector', { nameConnector: name });

    }

    validateUser() {
        let userName = 'Lattice-Jaya-POC-4';//User already created
        // let userName = 'k9adsbgl';// User id for M21BugBash1
        let observer = new Observer(
            response => {
                // httpService.printObservables();
                console.log('HEY ', response);
                if (response.data && response.data.name) {
                    this.setState({ userValidated: true, userInfo: response.data });
                    httpService.unsubscribeObservable(observer);
                } else {
                    this.setState({ userValidated: false, userInfo: {} });
                }
            },
            error => {
                console.error('ERROR ', error);
                this.setState({ userValidated: false });
            }
        );

        httpService.get(('/tray/user?userName=' + userName), observer);
    }

    getConnectros() {
        console.log('STATE', this.connectors);

        let connectors = this.connectors.map((obj, index) => {
            return (
                <Connector
                    key={index}
                    name={obj.name}
                    config={obj.config}
                    clickHandler={this.clickHandler}
                    classNames={`${"le-connector"} ${this.state.connectorSelected == obj.name ? 'selected' : ''}`}
                />
            );
        });
        return connectors;
    }

    render() {
        return (
            <div className="main-panel">
                <LeHPanel hstretch={"true"} halignment={CENTER}>
                    <h2 className="connectors-title">Select one of our application connectors</h2>
                </LeHPanel>
                <LeVPanel halignment={CENTER}>
                    
                    <LeHPanel hstretch={"true"} halignment={CENTER} valignment={CENTER}>
                        {/* <button>L</button> */}
                        <LeHPanel hstretch={"false"} halignment={CENTER} classesName="connectors-list">
                            {this.getConnectros()}
                        </LeHPanel>
                        {/* <button>R</button> */}
                    </LeHPanel>

                </LeVPanel>
                <LeToolBar direction={HORIZONTAL}>
                    <div className="right">
                        <LeButton
                            name="credentials"
                            disabled={!(ConnectorService.isUserValidated()) && this.state.connectorSelected == ''}
                            config={{
                                label: "Create",
                                classNames: "gray-button"
                            }}
                            callback={this.generateAuthTokenClickHandler}
                        />
                    </div>
                </LeToolBar>
            </div>
        );
    }
}

angular
    .module("le.connectors.list", ['lp.sfdc', 'mainApp.core.utilities.BrowserStorageUtility', 'mainApp.core.services.FeatureFlagService', 'common.modal'])
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
            return connectorsEnabled;
        }
    })
    .component(
        "connectorListComponent",
        react2angular(ConnectorList, [], ['$state', 'ConnectorsService'])
    );