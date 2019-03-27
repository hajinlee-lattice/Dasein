import React, { Component, react2angular } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";

class SystemsComponent extends Component {
    constructor(props) {
        super(props);

    }

    render() {
        return (
            <LeVPanel hstretch={"true"}>
                <LeHPanel hstretch={"true"}>Connected Systems</LeHPanel>
            </LeVPanel>
        );
    }
}
export default SystemsComponent;