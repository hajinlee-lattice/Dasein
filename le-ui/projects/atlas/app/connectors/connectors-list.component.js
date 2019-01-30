
import React, { Component, react2angular } from "common/react-vendor";
import './connectors-list.scss';
import LeVPanel from 'common/widgets/container/le-v-panel';
import LeHPanel from 'common/widgets/container/le-h-panel';
import { CENTER, LEFT } from 'common/widgets/container/le-alignments';
import Connector from './connector.component';
import ConnectorsRoutes from "./react-routing";
export class ConnectorList extends Component {
    constructor(props) {
        super(props);
        console.log('THE PROPS ', props);
        this.clickHandler = this.clickHandler.bind(this);
        this.state = { connectorSelected: this.props.ConnectorsService.test()};
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
        this.router.stateService.go('profiles',{ nameConnector: this.state.connectorSelected});
    }
    clickHandler(name) {
        this.setState({ connectorSelected: name });
        let nameConnector = name;
        this.router.stateService.go('profiles', {tenantName: 'M21BugBash1', nameConnector: nameConnector});

    }
    com
    getConnectros() {
        console.log('STATE', this.state);
        let connectors = this.connectors.map((obj, index) => {
            return (
                <Connector
                    key={index}
                    name={obj.name}
                    config={obj.config}
                    clickHandler={this.clickHandler}
                    classNames={`${this.state.connectorSelected == obj.name ? 'selected': ''}`}
                />
            );
        });
        return connectors;
    }

    render() {
        return (
            <LeVPanel vstretch={"true"} hstretch={"true"} classesName="le-connectors white-background">

                <LeHPanel hstretch={"true"} halignment={CENTER}>
                    <h2>Select one of our many application connectors</h2>
                </LeHPanel>
                <LeHPanel hstretch={"true"} halignment={LEFT} classesName="connectors-list">
                    {this.getConnectros()}
                </LeHPanel>
            </LeVPanel >
        );
    }
}

angular
    .module("le.connectors.list", [])
    .service('ConnectorsService', function($state){
        let ConnectorsService = this;
        this.test = function(){
            console.log('Test', $state.router.locationConfig.$location.$$hash);
            let hash = $state.router.locationConfig.$location.$$hash;
            console.log(hash);
            let selected = '';
            if(hash != ''){
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