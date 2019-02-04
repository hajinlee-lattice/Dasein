import React, { Component } from "common/react-vendor";
import ConnectorsRoutes from "./connectors-routing";

import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import LeTile from 'common/widgets/container/tile/le-tile';
import LeTileHeader from 'common/widgets/container/tile/le-tile-header';
import LeTileBody from 'common/widgets/container/tile/le-tile-body';
import LeMenu from 'common/widgets/menu/le-menu';
import LeHPanel from 'common/widgets/container/le-h-panel';
import Aux from "common/widgets/hoc/_Aux";
import {SPACEBETWEEN} from "common/widgets/container/le-alignments";
// import ConnectorsService, { trayAPI, User } from './connectors.service';
export default class ProfilesComponent extends Component {

    constructor(props) {
        super(props);
        this.state = {
            nameConnector: '',
            loading: false,
            connectors: []
        }

    }

    getLoading() {
        return (
            <div className="loading-container">
                <i className="fa fa-spinner fa-spin fa-2x fa-fw" />
            </div>
        );

    }

    getProfileUI(profileObj) {
        console.log('THE OBJ', profileObj);
        return (
            <LeTile classNames="profile-tile">
                <LeTileHeader>
                    <span className="le-tile-title">{profileObj.orgName}</span>
                    <LeMenu classNames="personalMenu" image="fa fa-ellipsis-v" name="main">

                    </LeMenu>
                </LeTileHeader>
                <LeTileBody>
                    <p>The description here</p>
                    <span>The body</span>
                </LeTileBody>
            </LeTile>
        );
    }
    getProfilesUI() {
        console.log('CREATING ', this.state.connectors);
        if (this.state.connectors && this.state.connectors.length > 0) {
            let systems = Object.keys(this.state.connectors).map(
                (obj, index) => {

                    return (
                        <Aux>{this.getProfileUI(this.state.connectors[obj])}</Aux>
                    );
                }
            );
            return systems;
        } else {
            return null;
        }
    }
    // getProfilesUI(){
    //     return (<div>
    //         {this.getProfileUI()}
    //     </div>);
    // }
    getProfiles() {
        if (this.state.loading === false) {
            return this.getProfilesUI();
        }
        if (this.state.loading === true) {
            // this.validateUser();
            return this.getLoading();
        }
    }

    componentDidMount() {
        console.log('PROFILE TRY');
        this.router = ConnectorsRoutes.getRouter();
        // this.setState({ nameConnector: this.router.stateService.params.nameConnector, loading: true });
        // httpService.get()
        httpService.get(
            "/pls/lookup-id-mapping",
            new Observer(response => {
                let connectors = response.data.CRM;
                connectors = connectors.concat(response.data.MAP);
                this.setState({ nameConnector: this.router.stateService.params.nameConnector, loading: false, connectors: connectors });
                console.log("BACK HERE ", response);
            })
        );
    }

    render() {
        return (
            <LeHPanel classesName="profiles" hstretch={"true"} wrap halignment={SPACEBETWEEN}>
                {this.getProfiles()}
            </LeHPanel>
        );
    }
}