import React, { Component, react2angular } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import './system-mapping.component.scss';

export default class SystemMappingComponent extends Component {

    constructor(props) {
        super(props);
        console.log('THE MAPPING', this.props);
        this.state = { systemsAvailable: [], system: props.system };
        this.accountIdClickHandler = this.accountIdClickHandler.bind(this);
    }

    componentWillUnmount(){
        // this.props.system = this.state.system;
        this.props.closed(this.state.system);
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

    accountIdClickHandler(event) {
        let systemCopy = Object.assign({}, this.state.system);
        console.log('COPY IN MAPPING', systemCopy);
        systemCopy.accountId = event.target.value;
        this.setState({system: systemCopy});
    }
    getAccountIDSelection() {
        if (this.state.accountIdSelectionLoaded) {
            return (
                <select value={this.state.system.accountId} onChange={this.accountIdClickHandler}>
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
                <div className="le-flex-row">
                    <div className="le-flex-column">
                        System Org ID:
                            </div>
                    <div className="le-flex-column color-blue">
                        {this.props.system.orgId}
                    </div>
                </div>
                <div className="le-flex-row">
                    <div className="le-flex-column">
                        System Org Name:
                            </div>
                    <div className="le-flex-column color-blue">
                        {this.props.system.orgName}
                    </div>
                </div>
                <div className="le-flex-row">
                    <div className="le-flex-column">
                        Account ID:
                            </div>
                    <div className="le-flex-column">
                        {this.getAccountIDSelection()}
                    </div>
                </div>
            </div>

        );
    }
}
