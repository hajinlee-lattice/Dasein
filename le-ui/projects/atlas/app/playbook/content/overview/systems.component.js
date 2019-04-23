import React, { Component, react2angular } from "common/react-vendor";
import Aux from 'common/widgets/hoc/_Aux';
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
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

/**
 * need time utility
 * need momentjs
 */ 

class SystemsComponent extends Component {
    constructor(props) {
        super(props);
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

    getLaunchStateText(launch) {
        var launchState = (launch ? launch.launchState : 'Unlaunched'),
            launched = (launchState === 'Launched' ? true : false),
            text = [];

        if(launched) {
            text.push(
                <div class="launch-text launched">
                    <h3>Last Launch settings</h3>
                    <ul>
                        <li>
                            Yesterday at 9:00am (created)?
                        </li>
                        <li>
                            Automatically every 2 months?
                        </li>
                        <li>
                            Destination: {launch.folderName}
                        </li>
                        <li>
                            Contacts: {launch.contactsLaunched.toLocaleString()}
                        </li>
                    </ul>
                </div>
            );
        } else {
            text.push(
                <div class="launch-text unlaunched">
                    No previous launch.
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

    makeConnections(connections) {
        var connections = this.getConnectionsList(connections),
            connectionTemplates = [];
        connections.forEach(function(connection) {
            connectionTemplates.push(this.connectionTemplate(connection))
        }, this);
        return connectionTemplates;
    }
    
    connectionTemplate(connection) {
        var connectionsTemplate = [];
        if(connection) {
            var launchState = (connection.launchConfiguration ? connection.launchConfiguration.launchState : 'Unlaunched'),
                launched = (launchState === 'Launched' ? true : false);

            return (
                <LeCard>
                    <div class="card-image-container">
                        <LeCardImg src={connection.config.img} contentAlignment={CENTER} />
                    </div>
                    <LeCardBody contentAlignment={CENTER}>
                        <ul>
                            <li>
                                <h2>{connection.orgName}</h2>
                            </li>
                            <li>
                                {this.getLaunchStateText(connection.launchConfiguration)}
                            </li>
                            <li>
                                <LeButton
                                    name="launch"
                                    disabled={launched}
                                    config={{
                                        label: "Launch",
                                        classNames: "orange-button"
                                    }}
                                    callback={() => { this.launchButtonClickHandler(connection) } }
                                />
                            </li>
                            <li>
                                <LeButton
                                    name="autolaunch"
                                    config={{
                                        label: "Auto Launch",
                                        classNames: "orange-button"
                                    }}
                                    callback={() => { this.launchButtonClickHandler(connection, true) } }
                                />
                            </li>
                        </ul>
                    </LeCardBody>
                </LeCard>
            );
        }
    }
    
    launchButtonClickHandler(connection, auto) {
        var auto = auto || false;
        console.log('okay then', {connection: connection, auto: auto});
    }

    render() {
        return (
            <Aux>
                <div class="connected-systems">
                    <h2>Connected Systems</h2>
                    <p>Activate a system to automate sending accounts and contacts.</p>
                    <GridLayout classNames="systems-grid extend">
                        {this.makeConnections(this.props.connections)}
                    </GridLayout>
                </div>
            </Aux>
        );
    }
}
export default SystemsComponent;