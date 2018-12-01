import React, { Component } from "common/react-vendor";
import { getRouter } from "./routing-test";

export default class Navigation extends Component {
    constructor(props) {
        super(props);
        this.callbackHandler = this.callbackHandler.bind(this);
    }

    callbackHandler() {
        console.log("OK");
        getRouter().stateService.go("hello");
    }

    render() {
        return (
            <div className="le-flex-h-panel">
                <button onClick={this.callbackHandler}>Click</button>
            </div>
        );
    }
}
