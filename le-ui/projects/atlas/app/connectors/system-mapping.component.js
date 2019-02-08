import React, { Component, react2angular } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import './system-mapping.component.scss';

export default class SystemMappingComponent extends Component {

    constructor(props) {
        super(props);
        this.state = { systemsAvailable: [] };
    }

    componentDidMount() {
        let observer = new Observer(
            response => {
                // httpService.printObservables();
                console.log('HEY ', response);
                if (response.data) {
                    let tmp = response.data;
                    let data = [];
                    Object.keys(tmp).forEach(element => {
                        let t = tmp[element];
                        data = data.concat(t);
                        // data.push(response.data[element]);
                    });
                    console.log('????????????', data);

                    this.setState({ systemsAvailable: data, accountIdSelectionLoaded: true });
                    httpService.unsubscribeObservable(observer);
                }
            }
        );

        httpService.get(('/pls/lookup-id-mapping/available-lookup-ids'), observer);
    }

    getSystemsAvailable() {
        let options = [];
        console.log('AAAA ===> ', this.state.systemsAvailable);
        this.state.systemsAvailable.forEach((element, index) => {
            options.push(
                <option value={element.fieldName} key={index}>{element.displayName}</option>
            );
        });
        return options;
    }

    getAccountIDSelection() {
        if (this.state.accountIdSelectionLoaded) {
            return (
                <select value={this.props.system.accountId}>
                    {this.getSystemsAvailable()}
                </select>
            );
        } else {
            return (<i class="fa fa-spinner fa-spin fa-fw" />);
        }
    }

    render() {
        console.log('RENDER MAPPING');
        return (

            <div className="system-mapping some-table">
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
                        {this.getAccountIDSelection()}
                    </div>
                </div>
            </div>

        );
    }
}
