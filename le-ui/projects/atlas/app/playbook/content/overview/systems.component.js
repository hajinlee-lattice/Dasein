import React, { Component } from "common/react-vendor";
import Aux from 'common/widgets/hoc/_Aux';
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import './systems.component.scss';
import { actions as modalActions } from 'common/widgets/modal/le-modal.redux';
import { actions } from '../../playbook.redux';
import { store } from 'store';
import LaunchComponent from "./launch.component";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";
import GridLayout from 'common/widgets/container/grid-layout.component';
import LeCard from 'common/widgets/container/card/le-card';
import LeCardImg from 'common/widgets/container/card/le-card-img';
import LeCardBody from 'common/widgets/container/card/le-card-body';
import LeButton from "common/widgets/buttons/le-button";
import {
  LEFT,
  CENTER,
  SPACEAROUND,
  SPACEBETWEEN,
  SPACEEVEN
} from "common/widgets/container/le-alignments";
import { LARGE_SIZE, MEDIUM_SIZE } from "common/widgets/modal/le-modal.utils";

/**
 * need time utility
 * need momentjs
 */ 

class SystemsComponent extends Component {
    constructor(props) {
        super(props);

        this.state = {
            refresh: false,
            play: props.play,
            connections: props.connections
        };

        this._connectors = {
            Salesforce: {
                name: 'Salesforce',
                config: { img: '/atlas/assets/images/logo_salesForce_2.png', text: 'Send and receive reccomandations about how likely leads, accounts and customers are to buy, what they are likely to buy and when, by connecting to this CRM' }
            },
            Marketo: {
                name: 'Marketo',
                config: { img: '/atlas/assets/images/logo_marketo_2.png', text: 'Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Marketo' }
            },
            Eloqua: {
                name: 'Eloqua',
                config: { img: '/atlas/assets/images/eloqua.png', text: 'Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Eloqua' }
            }
        };
    }

    componentDidMount() {
        let playstore = store.getState()['playbook'];

        actions.fetchRatings([playstore.play.ratingEngine.id], false);

        this.unsubscribe = store.subscribe(this.handleChange);
    }

    componentWillUnmount() {
      this.unsubscribe();
    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState(state);
    }

    getLaunchStateText(launch) {
        var launchState = (launch ? launch.launchState : 'Unlaunched'),
            launched = (launchState === 'Launched' ? true : false),
            text = [];

        if(launched) {
            text.push(
                <div class="launch-text launched">
                    <ul>
                        <li>
                            Last Launched: {new Date(launch.created).toLocaleDateString("en-US")}
                        </li>
                        <li>
                            Accounts Sent: {launch.accountsLaunched.toLocaleString()}
                        </li>
                        <li>
                            Contacts Sent: {launch.contactsLaunched.toLocaleString()}
                        </li>
                    </ul>
                </div>
            );
        } else {
            text.push(
                <div class="launch-text unlaunched">
                    No previous launch
                </div>
            );
        }
        return text;
    }

    getConnectionsList(connections) {
        if(connections) {
            var connectionsAr = [],
                connectionsList = [];

            if(connections.uniqueLookupIdMapping) {
                for(var i in connections.uniqueLookupIdMapping) {
                    connectionsAr = connectionsAr.concat(connections.uniqueLookupIdMapping[i]);
                }
            }
            connectionsAr.forEach(function(connection){
                connection.config = (this._connectors[connection.externalSystemName] ? this._connectors[connection.externalSystemName].config : {});
                connection.launchConfiguration = this.props.connections.launchConfigurations[connection.orgId];

                var launchState = (connection.launchConfiguration ? connection.launchConfiguration.launchState : 'Unlaunched'),
                    launched = (launchState === 'Launched' ? true : false);

                connectionsList.push(connection);
            }, this);

            return connectionsList;
        }
    }

    makeConnections(connections, play) {
        var connections = this.getConnectionsList(connections),
            connectionTemplates = [];
        connections.forEach(function(connection) {
            connectionTemplates.push(this.connectionTemplate(connection, play))
        }, this);
        return connectionTemplates;
    }
    
    connectionTemplate(connection, play) {
        var connectionsTemplate = [];
        if(connection) {
            var launchState = (connection.launchConfiguration ? connection.launchConfiguration.launchState : 'Unlaunched'),
                launched = (launchState === 'Launching' ? true : false);

            return (
                <LeHPanel hstretch={"true"} className={'connection-card'}>
                    <div class="connection-logo">
                        <img src={connection.config.img} />
                        <h2>{connection.orgName}</h2>
                    </div>
                    <div class="connection-info">
                        {this.getLaunchStateText(connection.launchConfiguration)}
                    </div>
                    <div class="connection-launch">
                        <LeButton
                            name="launch"
                            disabled={launched}
                            config={{
                                label: "Launch",
                                classNames: "orange-button"
                            }}
                            callback={() => {this.launchButtonClickHandler(connection, play)} } />
                    </div>
                </LeHPanel>
            );
        }
    }

    launchButtonClickHandler(connection, play) {
        let config = {
            callback: (action) => {
                modalActions.closeModal(store);
            },
            className: 'rating-modal',
            template: () => {
                function closeModal() {
                    modalActions.closeModal(store);
                }

                return (
                    <LaunchComponent closeFn={closeModal} connection={connection} play={this.state.play} />
                );
            },
            title: () => {
                return (
                    <p>Launch to {connection.orgName}</p>
                );
            },
            titleIcon: () => {
                let src = (this._connectors[connection.externalSystemName] ? this._connectors[connection.externalSystemName].config.img : '');
                return (
                    <img src={src} />
                );
            },
            hideFooter: true,
            size: LARGE_SIZE
        }
        modalActions.openModal(store, config);
    }

    render() {
        if(this.state.connections) {
            return (
                <div class="connected-systems">
                    <h2>Connected Systems</h2>
                    <p>Activate a system to automate sending accounts and contacts.</p>
                    <LeVPanel hstretch={"true"} className={'systems-grid'}>
                        {this.makeConnections(this.props.connections, this.props.play)}
                    </LeVPanel>
                </div>
            );
        } else {
            return (
                <Aux>
                    <p>Loading...</p>
                </Aux>
            );
        }
    }
}
export default SystemsComponent;