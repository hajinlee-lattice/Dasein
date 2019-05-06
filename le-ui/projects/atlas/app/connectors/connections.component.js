import React, { Component } from "common/react-vendor";
import { store, injectAsyncReducer } from 'store';

import { CENTER, LEFT } from 'common/widgets/container/le-alignments';
import { openConfigWindow, solutionInstanceConfig } from "./configWindow";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

import LeButton from "common/widgets/buttons/le-button";
import ReactRouter from 'atlas/react/router';
import './connections.component.scss';
import LeVPanel from 'common/widgets/container/le-v-panel';
import LeHPanel from 'common/widgets/container/le-h-panel';
import { LeToolBar, HORIZONTAL } from 'common/widgets/toolbar/le-toolbar';
import Connector from './connector.component';
import { actions, reducer } from './connections.redux';


import ConnectorService, { MARKETO, SALESFORCE, ELOQUA } from './connectors.service';

import SystemsListComponent from './systems-list.component';
import ReactMainContainer from "../react/react-main-container";

export default class ConnectionsComponent extends Component {
    constructor(props) {
        super(props);
        this.ConnectorsService = ReactRouter.getRouter().ngservices.ConnectorsService;
        this.generateAuthTokenClickHandler = this.generateAuthTokenClickHandler.bind(this);
        this.clickHandler = this.clickHandler.bind(this);
        this.state = {
            connectorSelected: '',
            userValidated: false,
            userInfo: null,
            userName: null,
            accessToken: null,
            authorizationCode: null,
            solutionInstanceId: null,
            openModal: false
        };
        this.connectors = this.connectors = ConnectorService.getList(this.ConnectorsService.isMarketoEnabled());
        // console.log('UUUUUUU ',ReactRouter.getRouter().ngservices.ConnectorsService.isMarketoEnabled());
    }

    handleChange = () => {
        const data = store.getState()['connections'];
        console.log(data);
        let userName = data.userName;
        let userId = data.userId;
        let accessToken = data.accessToken;
        this.setState({
            userName: userName,
            accessToken: accessToken,
            userValidated: userName != null && userName != undefined
        });
        if (userName && userId && accessToken) {
            this.setState({
                userValidated: true,
                userInfo: {
                    userName: userName,
                    id: userId,
                    accessToken: accessToken
                }
            });
        }
    }

    componentDidMount() {
        injectAsyncReducer(store, 'connections', reducer);
        this.unsubscribe = store.subscribe(this.handleChange);

        // this.getTrayUserName();
        this.router = ReactRouter.getRouter();
        if (name != MARKETO) {
            ConnectorService.setUserValidated(true);
        } else {
            this.validateUser(userName);
        }
    }

    clickHandler(name) {
        console.log(name);
        this.setState({ connectorSelected: name });
        ConnectorService.setConnectorName(name);
        if (name != MARKETO) {
            ConnectorService.setUserValidated(true);
        } else if (name == MARKETO && this.state.userInfo == null) {
            ConnectorService.setUserValidated(true);
        }

    }
    generateAuthTokenClickHandler() {
        if (ConnectorService.getConnectorName() != '' && ConnectorService.getConnectorName() != MARKETO) {
            ConnectorService.sendMSG(() => {
                this.ConnectorsService.generateAuthToken();
            });
        } else if (ConnectorService.getConnectorName() == MARKETO) {
            this.getSolutionConfiguration(this.state.userInfo.id, MARKETO, this.state.userName + '_' + MARKETO + '_' + (new Date()).getTime());
        }

    }

    validateUser(userName) {
        console.log(userName);
        let observer = new Observer(
            response => {
                // httpService.printObservables();
                if (response.data && response.data.name) {
                    this.setState({ userValidated: true, userInfo: response.data, accessToken: response.data.accessToken });
                    solutionInstanceConfig.accessToken = this.state.accessToken;
                    console.log(!(ConnectorService.isUserValidated()) && this.state.connectorSelected == '' || (this.state.connectorSelected == 'Marketo' && (!this.state.accessToken || !this.state.userInfo)));
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

    // getTrayUserName() {
    //     let observer = new Observer(
    //         response => {
    //             // httpService.printObservables();
    //             if (response.data && response.data.DropBox) {
    //                 let userName = response.data.DropBox;
    //                 this.setState({ userName: userName });
    //                 httpService.unsubscribeObservable(observer);
    //             } else {
    //                 this.setState({ userName: null });
    //             }
    //         },
    //         error => {
    //             console.error('ERROR ', error);
    //             this.setState({ userName: null });
    //         }
    //     );

    //     httpService.get('/pls/dropbox/summary', observer);
    // }

    getSolutionConfiguration(userId, tag, instanceName) {
        const configWindow = openConfigWindow();
        solutionInstanceConfig.orgType = tag;
        let observer = new Observer(
            response => {
                if (response.data && response.data.authorizationCode) {
                    var data = response.data;
                    this.setState({authorizationCode: data.authorizationCode, solutionInstanceId: data.solutionInstanceId});
                    solutionInstanceConfig.id = data.solutionInstanceId;
                    solutionInstanceConfig.registerLookupIdMap = true;
                    solutionInstanceConfig.accessToken = this.state.accessToken;
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
        httpService.get('/tray/solutionconfiguration?tag=' + tag + '&userId=' + userId + '&instanceName=' + instanceName, observer, {UserAccessToken: userAccessToken});
    }

    // getUserAccessToken(userId) {
    //     let observer = new Observer(
    //         response => {
    //             if (response.data) {
    //                 this.setState({accessToken: response.data.token});
    //                 solutionInstanceConfig.accessToken = this.state.accessToken;
    //                 console.log(!(ConnectorService.isUserValidated()) && this.state.connectorSelected == '' || (this.state.connectorSelected == 'Marketo' && (!this.state.accessToken || !this.state.userInfo)));
    //                 httpService.unsubscribeObservable(observer);
    //             } else {
    //                 this.setState({accessToken: null});
    //             }
    //         },
    //         error => {
    //             this.setState({accessToken: null});
    //         }
    //     );

    //     httpService.post('/tray/authorize?userId=' + userId, {}, observer);
    // }

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
        return `https://app.tray.io/external/solutions/${partnerId}/configure/${solutionInstanceId}?code=${authorizationCode}&customValidation=true`;
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
            <ReactMainContainer>
            <LeVPanel hstrech={'true'}>
                <h2 className="connectors-title">Add a new connection</h2>
                <LeVPanel halignment={CENTER}>
                    
                    <LeHPanel hstretch={"true"} halignment={CENTER} valignment={CENTER}>
                        {/* <button>L</button> */}
                        <LeHPanel hstretch={"false"} halignment={CENTER} className="connectors-list">
                            {this.getConnectros()}
                        </LeHPanel>
                        {/* <button>R</button> */}
                    </LeHPanel>
                </LeVPanel>
                <LeToolBar direction={HORIZONTAL} className={"strech"}>
                    <div className="right">
                        <LeButton
                            name="credentials"
                            disabled={!(ConnectorService.isUserValidated()) && this.state.connectorSelected == '' || (this.state.connectorSelected == 'Marketo' && (!this.state.accessToken || !this.state.userInfo))}
                            config={{
                                label: "Create",
                                classNames: "gray-button aptrinsic-connections-create-system"
                            }}
                            callback={this.generateAuthTokenClickHandler}
                        />
                    </div>
                </LeToolBar>
                <SystemsListComponent />
            </LeVPanel>
            </ReactMainContainer>
        );
    }
}