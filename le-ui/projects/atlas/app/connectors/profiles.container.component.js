
import React, { Component, react2angular } from "common/react-vendor";
import './connectors-list.scss';
import './profiles.connector.scss';
import ConnectorsRoutes from "./react-routing";
import { UIRouter, UIView } from "common/react-vendor";

export class ProfilesContainerComponent extends Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <div className="main-panel">
                <div className="connector-profile-container">
                    <UIRouter router={ConnectorsRoutes.getRouter()}>
                        <UIView name="mainreact" />
                    </UIRouter>
                </div>
            </div>
        );
    }
}

angular
    .module("le.connectors.profile", [])
    .component(
        "profilesContainerComponent",
        react2angular(ProfilesContainerComponent, [], ["$state"])
    );