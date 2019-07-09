import React, { Component } from "common/react-vendor";
import Aux from 'common/widgets/hoc/_Aux';
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import './systems.component.scss';
import { actions as modalActions } from 'common/widgets/modal/le-modal.redux';
import { actions } from '../../playbook.redux';
import { store } from 'store';
import { debounce } from 'lodash';
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
            connections: null,
            checkLaunch: null
        };

        this._connectors = {
            Salesforce: {
                name: 'Salesforce',
                config: { 
                    name: 'Salesforce',
                    img: '/atlas/assets/images/logo_salesForce_2.png', 
                    text: 'Send and receive reccomandations about how likely leads, accounts and customers are to buy, what they are likely to buy and when, by connecting to this CRM' 
                }
            },
            Marketo: {
                name: 'Marketo',
                config: { 
                    name: 'Marketo',
                    img: '/atlas/assets/images/logo_marketo_2.png', 
                    text: 'Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Marketo' 
                }
            },
            Eloqua: {
                name: 'Eloqua',
                config: { 
                    name: 'Eloqua',
                    img: '/atlas/assets/images/eloqua.png', 
                    text: 'Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to Eloqua' 
                }
            },
            AWS_S3:  {
                name: 'AWS_S3',
                config: { 
                    name: 'AWS S3',
                    img: '/atlas/assets/images/logo_aws_s3.png', 
                    text: 'Activate audience segments based on your Customer 360 data to power your email campaigns, by connecting to AWS S3' 
                }
            }
        };
    }

    componentDidMount() {
        let playstore = store.getState()['playbook'];

        if(playstore.play.ratingEngine) {
            actions.fetchRatings([playstore.play.ratingEngine.id], false);
        }

        actions.fetchLookupIdMapping();

        if(playstore.play.name) {
            if(!playstore.connections) {
                actions.fetchConnections(playstore.play.name);
            } else {
                this.setState({connections: playstore.connections});
            }
        }

        this.unsubscribe = store.subscribe(this.handleChange);
    }

    componentWillUnmount() {
      this.unsubscribe();
      clearInterval(this.state.checkLaunch);
    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState(state);
    }


    checkLaunching() {
        if(this.state.checkLaunch) {
            return false;
        }
        var interval = .1 * (1000 * 60),
            vm = this,
            checkLaunch = setInterval(function() {
                let playstore = store.getState()['playbook'];

                actions.fetchConnections(playstore.play.name, true);

                let connections = playstore.connections,
                    launchingConnection = connections.find(function(connection) {
                        return (connection  && connection.lastLaunch && connection.lastLaunch.launchState === 'Launching');
                    });

                if(!launchingConnection) {
                    clearInterval(checkLaunch);
                    vm.setState({checkLaunch: null});
                }
            }, interval);

        this.setState({checkLaunch: checkLaunch});
    }

    getLaunchStateText(connection, play) {
        var text = [];
        if(connection && connection.lastLaunch && connection.created && connection.accountsLaunched && connection.contactsLaunched) {
            var launch = connection.lastLaunch,
                launchState = (launch ? launch.launchState : 'Unlaunched'),
                launched = (launchState === 'Launched' ? true : false),
                launching = (launchState === 'Launching' ? true : false);

                if(launching) {
                    this.checkLaunching();
                }

            if(launched) {
                text.push(
                    <div class="launch-text launched">
                        <ul>
                            <li>
                                Last Launched: {new Date(connection.created).toLocaleDateString("en-US")}
                            </li>
                            <li>
                                Accounts Sent: {connection.accountsLaunched.toLocaleString()}
                            </li>
                            <li>
                                Contacts Sent: {connection.contactsLaunched.toLocaleString()}
                            </li>
                        </ul>
                    </div>
                );
            } else if(launching) {
                text.push(
                    <div class="launch-text unlaunched">
                        Launching...
                    </div>
                );
            } else {
                text.push(
                    <div class="launch-text unlaunched">
                        No previous launch
                    </div>
                );
            }
        }
        return text;
    }

    getLaunchButton(connection, play) {
        var button = [],
            launch = connection.lastLaunch,
            launchState = (launch ? launch.launchState : 'Unlaunched'),
            launched = (launchState === 'Launched' ? true : false),
            launching =  (launchState === 'Launching' ? true : false),
            active = false;

        var activeState = (connection.isAlwaysOn ? 'Active' : 'Inactive');
        if(connection.isAlwaysOn) {
            button.push(
                <LeButton
                    name="activate"
                    config={{
                        label: activeState,
                        classNames: `borderless-button campaign-launch-button activate ${activeState}`
                    }}
                    callback={() => {this.activateButtonClickHandler(connection, play)} } />
            );
        } else {
            button.push(
                <LeButton
                    name="launch"
                    disabled={launching}
                    config={{
                        label: "Ready to launch",
                        classNames: "borderless-button campaign-launch-button launch"
                    }}
                    callback={() => {this.launchButtonClickHandler(connection, play)} } />
            );
        }
        return button;
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
                connection.launchConfiguration = this.state.connections.launchConfigurations[connection.orgId];

                var launchState = (connection.launchConfiguration ? connection.launchConfiguration.launchState : 'Unlaunched'),
                    launched = (launchState === 'Launched' ? true : false);

                connectionsList.push(connection);
            }, this);

            return connectionsList;
        }
    }

    makeConnections(connections, play) {
        //var connections = this.getConnectionsList(connections),
        var connectionTemplates = [];
        connections.forEach(function(connection) {
            connectionTemplates.push(this.connectionTemplate(connection, play))
        }, this);
        return connectionTemplates;
    }
    
    connectionTemplate(connection, play) {
        var connectionsTemplate = [];
        if(connection) {
            var launchState = (connection.lastLaunch ? connection.lastLaunch.launchState : 'Unlaunched'),
                launched = (launchState === 'Launching' ? true : false);

            var configObj = this._connectors[connection.lookupIdMap.externalSystemName],
                config = (configObj ? configObj.config : {}),
                activeState = (connection.isAlwaysOn ? 'Active' : 'Inactive');

            return (
                <LeHPanel hstretch={"true"} className={`connection-card ${activeState}`}>
                    <div class="connection-logo">
                        <img src={config.img} />
                        <h2 title={connection.lookupIdMap.orgName}>{connection.lookupIdMap.orgName}</h2>
                    </div>
                    <div class="connection-info">
                        {this.getLaunchStateText(connection, play)}
                    </div>
                    <div class="connection-launch">
                        {this.getLaunchButton(connection, play)}
                    </div>
                </LeHPanel>
            );
        }
    }

    activateButtonClickHandler(connection, play) {
        actions.saveChannel(play.name, {
            id: connection.id,
            lookupIdMap: connection.lookupIdMap,
            isAlwaysOn: !connection.isAlwaysOn,
            channelConfig: connection.channelConfig
        });
    }

    launchButtonClickHandler(connection, play) {
        let config = {
            callback: (action) => {
                modalActions.closeModal(store);
            },
            className: 'launch-modal',
            template: () => {
                function closeModal() {
                    modalActions.closeModal(store);
                }

                let configObj = this._connectors[connection.lookupIdMap.externalSystemName],
                    config = (configObj ? configObj.config : {});

                return (
                    <LaunchComponent closeFn={closeModal} connection={connection} play={this.state.play} config={config} />
                );
            },
            title: () => {
                var title = `Launch to ${connection.lookupIdMap.orgName}`;
                return (
                    <p title={title}>{title}</p>
                );
            },
            titleIcon: () => {
                let src = (this._connectors[connection.lookupIdMap.externalSystemName] ? this._connectors[connection.lookupIdMap.externalSystemName].config.img : '');
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
                    <h2 className="panel-label">Channels</h2>
                    <LeVPanel hstretch={"true"} className={'systems-grid'}>
                        {this.makeConnections(this.state.connections, this.props.play)}
                    </LeVPanel>
                </div>
            );
        } else {
            return (
                <Aux>
                    <div className="loader"></div>
                </Aux>
            );
        }
    }
}
export default SystemsComponent;