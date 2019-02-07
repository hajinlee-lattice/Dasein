import React, { Component, react2angular } from "common/react-vendor";
import './system-mapping.component.scss';

export default class SystemMappingComponent extends Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (

            <div className="some-table">
                <div className="row">
                    <div className="column">
                        System Org ID:
                            </div>
                    <div className="column color-blue">
                        {this.props.system.orgId}
                    </div>
                </div>
                <div className="row">
                    <div className="column">
                        System Org Name:
                            </div>
                    <div className="column color-blue">
                        {this.props.system.orgName}
                    </div>
                </div>
                <div className="row">
                    <div className="column">
                        Account ID:
                            </div>
                    <div className="column">
                        <select>
                            <option>Test1</option>
                        </select>
                    </div>
                </div>
            </div>

        );
    }
}
