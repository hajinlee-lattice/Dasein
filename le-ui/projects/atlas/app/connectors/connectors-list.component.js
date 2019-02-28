
import React, { Component, react2angular } from "common/react-vendor";
import './connectors-list.scss';
import LeVPanel from 'common/widgets/container/le-v-panel';
import LeHPanel from 'common/widgets/container/le-h-panel';
import { LeToolBar, HORIZONTAL } from 'common/widgets/toolbar/le-toolbar';
import LeModal from "common/widgets/modal/le-modal";
import LeButton from "common/widgets/buttons/le-button";
import { CENTER, LEFT } from 'common/widgets/container/le-alignments';
import Connector from './connector.component';
import ConnectorsRoutes from "./connectors-routing";
import { openConfigWindow, solutionInstanceConfig } from "./configWindow";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

import ConnectorService, { MARKETO, SALESFORCE, ELOQUA } from './connectors.service';

export class ConnectorList extends Component {
    constructor(props) {
        super(props);
        // console.log('THE PROPS ', props);
        this.clickHandler = this.clickHandler.bind(this);
        this.generateAuthTokenClickHandler = this.generateAuthTokenClickHandler.bind(this);
        this.modalCallback = this.modalCallback.bind(this);
        this.getIFrame = this.getIFrame.bind(this);
        this.state = {
            connectorSelected: this.props.ConnectorsService.getConnector(),
            userValidated: false,
            userInfo: null,
            userName: null,
            accessToken: null,
            authorizationCode: null,
            solutionInstanceId: null,
            openModal: false
        };
        this.connectors = ConnectorService.getList(this.props.ConnectorsService.isMarketoEnabled());

    }
    componentDidMount() {
        this.getTrayUserName(response => {
            console.log("getTrayUserName");
        });
        this.router = ConnectorsRoutes.getRouter();
        if (name != MARKETO) {
            ConnectorService.setUserValidated(true);
        } else {
            this.validateUser(userName);
        }

        this.router.stateService.go('profiles', { nameConnector: this.state.connectorSelected });
        ConnectorService.getList();

    }

    generateAuthTokenClickHandler() {
        if (ConnectorService.getConnectorName() != '' && ConnectorService.getConnectorName() != MARKETO) {
            ConnectorService.sendMSG(() => {
                this.props.ConnectorsService.generateAuthToken();
            });
        } else if (ConnectorService.getConnectorName() == MARKETO) {
            this.getSolutionConfiguration(this.state.userInfo.id, MARKETO, this.state.userName + '_' + MARKETO + '_' + (new Date()).getTime());
        }

    }

    clickHandler(name) {
        this.setState({ connectorSelected: name });
        ConnectorService.setConnectorName(name);
        if (name != MARKETO) {
            ConnectorService.setUserValidated(true);
        } else if (name == MARKETO && this.state.userInfo == null) {
            ConnectorService.setUserValidated(true);
            this.validateUser(this.state.userName)
        }
        this.router.stateService.go('profilesconnector', { nameConnector: name });

    }

    validateUser(userName) {
        let observer = new Observer(
            response => {
                // httpService.printObservables();
                if (response.data && response.data.name) {
                    this.setState({ userValidated: true, userInfo: response.data });
                    this.getUserAccessToken(response.data.id);
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

    getTrayUserName() {
        let observer = new Observer(
            response => {
                // httpService.printObservables();
                console.log("getUserName response: ", response);
                if (response.data && response.data.DropBox) {
                    let userName = response.data.DropBox;
                    this.setState({ userName: userName });
                    httpService.unsubscribeObservable(observer);
                } else {
                    this.setState({ userName: null });
                }
            },
            error => {
                console.error('ERROR ', error);
                this.setState({ userName: null });
            }
        );

        httpService.get('/pls/dropbox/summary', observer);
    }

    getSolutionConfiguration(userId, tag, instanceName) {
        const configWindow = openConfigWindow();
        let observer = new Observer(
            response => {
                if (response.data && response.data.authorizationCode) {
                    var data = response.data;
                    this.setState({authorizationCode: data.authorizationCode, solutionInstanceId: data.solutionInstanceId});
                    solutionInstanceConfig.id = data.solutionInstanceId;
                    configWindow.location = this.getPopupUrl(data.solutionInstanceId, data.authorizationCode);
                    // this.setState({openModal: true});
                    httpService.unsubscribeObservable(observer);
                } else {
                    console.log("ERROR");
                }
            },
            error => {
                console.error('ERROR ', error);
            }
        );
        let userAccessToken = this.state.accessToken;
        console.log("USER_ACCESS_TOKEN: " + userAccessToken);
        httpService.get('/tray/solutionconfiguration?tag=' + tag + '&userId=' + userId + '&instanceName=' + instanceName, observer, {UserAccessToken: userAccessToken});
    }

    getUserAccessToken(userId) {
        let observer = new Observer(
            response => {
                if (response.data) {
                    this.setState({accessToken: response.data.token});
                    httpService.unsubscribeObservable(observer);
                } else {
                    this.setState({accessToken: null});
                }
            },
            error => {
                this.setState({accessToken: null});
            }
        );

        httpService.post('/tray/authorize?userId=' + userId, {}, observer);
    }

    modalCallback(action) {
        switch (action) {
            case 'close':
                this.setState({ openModal: false });
                break;
            case 'ok':
                this.setState({ openModal: false, saving: true });
                break;
        }
    }

    registerLookupIdMapping() {
        let observer = new Observer(
            response => {
                // httpService.printObservables();
                if (response.data && response.data.name) {
                    console.log("HELLO");
                    console.log(response);
                    httpService.unsubscribeObservable(observer);
                } else {
                    console.log("response", response);
                }
            },
            error => {
                console.error('ERROR ', error);
            }
        );

        var lookupIdMap = {
            orgId: (new Date()).getTime(),
            orgName: "test",
            externalSystemType: "MAP",
            externalSystemName: "Marketo",
            externalAuthentication: {
                solutionInstanceId: this.state.solutionInstanceId,
                trayWorkflowEnabled: false
            }
        };

        console.log("lookupIdMap register: " + lookupIdMap);

        // httpService.post('/pls/lookup-id-mapping/register', lookupIdMap, observer);
    }

    getIFrame() {
        let partnerId = 'LatticeEngines';
        let url = `https://app.tray.io/external/solutions/${partnerId}/configure/${this.state.solutionInstanceId}?code=${this.state.authorizationCode}`;
        return (
            <div>
                <Iframe source={url} />
            </div>
        );
    }

    getPopupUrl(solutionInstanceId, authorizationCode) {
        let partnerId = 'LatticeEngines';
        return `https://app.tray.io/external/solutions/${partnerId}/configure/${solutionInstanceId}?code=${authorizationCode}`;
    }

    getPopup(solutionInstanceId, authorizationCode) {
        let partnerId = 'LatticeEngines';
        var url = `https://app.tray.io/external/solutions/${partnerId}/configure/${solutionInstanceId}?code=${authorizationCode}`;
        return (
            <div>
                <iframe src={url} width='500px'/>
            </div>
        );
    }



    getConnectros() {
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
                <LeModal opened={this.state.openModal} callback={this.modalCallback} title="Configuration Wizard" template={this.getIFrame} />
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
                            disabled={!(ConnectorService.isUserValidated()) && this.state.connectorSelected == '' || (this.state.connectorSelected == 'Marketo' && (!this.state.accessToken || !this.state.userInfo))}
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
    .module("le.connectors.list", ['lp.sfdc', 'common.utilities.browserstorage', 'common.services.featureflag', 'common.modal'])
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