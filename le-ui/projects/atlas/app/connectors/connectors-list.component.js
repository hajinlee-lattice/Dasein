
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

import ConnectorsService, { trayAPI, User } from './connectors.service';
export class ConnectorList extends Component {
    constructor(props) {
        super(props);
        console.log('THE PROPS ', props);
        this.clickHandler = this.clickHandler.bind(this);
        this.state = {
            connectorSelected: this.props.ConnectorsService.getConnector(),
            userValidated: false,
            userInfo: null
        };
        this.connectors = [
            {
                name: 'salesforce',
                config: { img: '/atlas/assets/images/logo_salesForce_2.png', text: 'Send and receive reccomandations about how likely leads, accounts and customers are to buy, what they are likely to buy and when, by connecting to this CRM' },
            },
            {
                name: 'marketo',
                config: { img: '/atlas/assets/images/logo_marketo_2.png', text: 'Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Marketo' }
            }
        ];

    }
    componentDidMount() {
        this.router = ConnectorsRoutes.getRouter();
        this.validateUser();
        this.router.stateService.go('profiles', { nameConnector: this.state.connectorSelected });
    }
    clickHandler(name) {
        this.setState({ connectorSelected: name });
        let nameConnector = name;
    }

    validateUser() {
        let userName = 'Lattice-Jaya-POC-4';//User already created
        // let userName = 'k9adsbgl';// User id for M21BugBash1
        let observer = new Observer(
            response => {
                httpService.printObservables();
                console.log('HEY ', response);
                if (response.data && response.data.name) {
                    this.setState({ userValidated: true, userInfo: response.data });
                    httpService.unsubscribeObservable(observer);
                    httpService.printObservables();
                } else {
                    this.setState({ userValidated: false, userInfo: new User('No user') });
                }
            },
            error => {
                console.error('ERROR ', error);
                this.setState({ userValidated: false });
            }
        );

        httpService.get(('/tray/user?userName='+userName), observer);
    }

    getConnectros() {
        console.log('STATE', this.state);
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
                        <h2 className="connectors-title">Select one of our many application connectors</h2>
                    </LeHPanel>

                    <LeHPanel hstretch={"true"} halignment={LEFT} classesName="connectors-list">
                        {this.getConnectros()}
                    </LeHPanel>
                    <LeToolBar direction={HORIZONTAL}>
                        <div className="right">
                            <LeButton
                                name="credentials"
                                disabled={!this.state.userValidated}
                                config={{
                                    label: "Create",
                                    classNames: "gray-button"
                                }}
                                callback={() => {
                                    // httpService.get(
                                    //     "/tray/user?userName=leonardo",
                                    //     new Observer(response => {
                                    //         console.log("BACK HERE ", response);
                                    //     })
                                    // );
                                }}
                            />
                        </div>
                    </LeToolBar>
            </div>
        );
    }
}

angular
    .module("le.connectors.list", [])
    .service('ConnectorsService', function ($state) {
        let ConnectorsService = this;
        this.getConnector = function () {
            console.log('Test', $state.router.locationConfig.$location.$$hash);
            let hash = $state.router.locationConfig.$location.$$hash;
            console.log(hash);
            let selected = '';
            if (hash != '') {
                let hashArray = hash.split('/');
                console.log('ARRAY ', hashArray[1]);
                selected = hashArray[1];
            }
            return selected;
        };
    })
    .component(
        "connectorListComponent",
        react2angular(ConnectorList, [], ['$state', 'ConnectorsService'])
    );