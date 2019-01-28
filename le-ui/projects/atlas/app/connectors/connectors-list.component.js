
import React, { Component, react2angular } from "../../../common/react-vendor";

export class ConnectorList extends Component {
    constructor() {
        super();
        console.log('LIST CONNECTORS');
    }
    render() {
        return( <div>
            <p>TEST</p>
        </div>);
    }
}

angular
    .module("le.connectors.list", [])
    .component(
        "connectorListComponent",
        react2angular(ConnectorList, [], ["$state"])
    );