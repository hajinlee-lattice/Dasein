import React, { Component } from "common/react-vendor";
import ConnectorsRoutes from "./connectors-routing";

import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import ConnectorsService, {trayAPI} from './connectors.service';
import ExternalIntegrationService from "./externalintegration.service"
export default class ProfilesComponent extends Component {

    constructor(props) {
        super(props);
        this.state = {
            nameConnector: '',
            userValidate: true
        }

    }

    getLoading() {
        return (
            <div className="loading-container">
                <i className="fa fa-spinner fa-spin fa-2x fa-fw" />
            </div>
        );

    }
    getProfileUI(){
        return (<div>{this.state.nameConnector}</div>);
    }
    getProfiles() {
        if (this.state.userValidate === true && this.state.nameConnector != '' || (this.state.userValidate === false && this.state.nameConnector == '')) {
            return this.getProfileUI();
        } 
        if(this.state.userValidate === false && this.state.nameConnector != '') {
            this.validateUser();
            return this.getLoading();
        }
    }

    componentDidMount() {
        this.router = ConnectorsRoutes.getRouter();
        this.externalIntegrationService = new ExternalIntegrationService();
        this.setState({ nameConnector: this.router.stateService.params.nameConnector, userValidate:false });
    }

    validateUser(){
        let userName = 'Lattice-Jaya-POC-4';//'k9adsbgl';
        let observer = new Observer(
            response => {
                httpService.printObservables();
                console.log('HEY ', response);
                if(response.data.users.edges.length > 0){
                    this.setState({userValidate: true});
                    httpService.unsubscribeObservable(observer);
                    httpService.printObservables();
                }else{
                    this.createUser(userName, observer);
                }
            },
            error => {
                console.error('ERROR ', error);
                this.setState({userValidate: true});
            }
        );
        let query = ConnectorsService.getUserQuery(userName);
        // httpService.post(trayAPI, query, observer);
        httpService.postGraphQl(trayAPI, query, observer);
    }
    createUser(userName, observer){


    }

    render() {
        return (
            <div>{this.getProfiles()}</div>
        );
    }
}