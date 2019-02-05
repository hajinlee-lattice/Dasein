
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
            connectorSelected: this.props.ConnectorsService.test(),
            userValidate: true,
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
        this.router.stateService.go('profiles');
    }
    clickHandler(name) {
        this.setState({ connectorSelected: name });
        let nameConnector = name;
        this.router.stateService.go('profilesconnector', { tenantName: 'M21BugBash1', nameConnector: nameConnector });

    }

    validateUser() {
        // let userName = 'Lattice-Jaya-POC-4';
        let userName = 'k9adsbgl';
        let observer = new Observer(
            response => {
                httpService.printObservables();
                console.log('HEY ', response);
                if (response.data.users.edges.length > 0) {
                    let userInfo = ConnectorsService.getUserInfo(response.data.users.edges);
                    this.setState({ userValidate: true, userInfo: userInfo });
                    httpService.unsubscribeObservable(observer);
                    httpService.printObservables();
                    this.router.stateService.go('profiles', { nameConnector: this.state.connectorSelected });
                } else {
                    this.setState({ userValidate: true, userInfo: new User('No user') });
                    this.createUser(userName, observer);
                }
            },
            error => {
                console.error('ERROR ', error);
                this.setState({ userValidate: true });
            }
        );
        let query = ConnectorsService.getUserQuery(userName);
        // httpService.post(trayAPI, query, observer);
        httpService.postGraphQl(trayAPI, query, observer);
    }
    createUser(userName, observer) {
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
                {/* // <LeVPanel vstretch={"true"} hstretch={"false"} classesName="le-connectors white-background"> */}

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
                                config={{
                                    label: "Create",
                                    classNames: "gray-button"
                                }}
                                callback={() => {
                                    // httpService.get(
                                    //     "/pls/lookup-id-mapping",
                                    //     new Observer(response => {
                                    //         // console.log("BACK HERE ", response);
                                    //     })
                                    // );
                                }}
                            />
                        </div>
                    </LeToolBar>
                {/* // </LeVPanel > */}
            </div>
        );
    }
}

angular
    .module("le.connectors.list", [])
    .service('ConnectorsService', function ($state) {
        let ConnectorsService = this;
        this.test = function () {
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