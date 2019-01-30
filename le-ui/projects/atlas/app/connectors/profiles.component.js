import React, { Component } from "common/react-vendor";

import { getRouter } from "./react-routing";

export default class ProfilesComponent extends Component {

    constructor(props) {
        super(props);
        this.state = {
            nameConnector : ''
        }
    }

    componentDidMount() {
        this.router = getRouter();
        this.setState({nameConnector : this.router.stateService.params.nameConnector});
    }
    
    render() {
        return (
            <div>{this.state.nameConnector}</div>
        );
    }
}