import React, { Component } from "common/react-vendor";
import './system-creation.component.scss';
import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";
import GridLayout, { MIN_GAP } from 'common/widgets/container/grid-layout.component';
import LeCard from 'common/widgets/container/card/le-card';
import LeCardImg from 'common/widgets/container/card/le-card-img';
import LeCardBody from 'common/widgets/container/card/le-card-body';
import { CENTER } from 'common/widgets/container/le-alignments';
import { LeToolBar, SPACE_BETWEEN } from "../../../../../common/widgets/toolbar/le-toolbar";
import LeButton from "../../../../../common/widgets/buttons/le-button";
import ReactRouter from 'atlas/react/router';
import LeInputText from "../../../../../common/widgets/inputs/le-input-text";
import ReactMainContainer from "../../../react/react-main-container";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
export default class SystemCreationComponent extends Component {
    constructor(props) {
        super(props);
        this.validate = this.validate.bind(this);
        this.systemsObj = [
            { name: 'Salesforce', img: '/atlas/assets/images/logo_salesForce_2.png', text: 'Text' },
            { name: 'Marketo', img: '/atlas/assets/images/logo_marketo_2.png', text: 'Text' },
            { name: 'Eloqua', img: '/atlas/assets/images/eloqua.png', text: 'Text' },
            { name: 'Others', img: '/atlas/assets/images/other_systems.png', text: 'Text' }
        ];
        this.state = { systemSelected: undefined, valid: false, newSystemName: '' };
    }

    componentDidMount() {

    }
    componentWillUnmount() {

    }
    validate() {
        if (this.state.systemSelected && this.state.newSystemName.trim() != '') {
            this.setState({ valid: true });
        } else {
            this.setState({ valid: false });
        }
    }

    getSystemSupported() {
        let systems = [];
        this.systemsObj.forEach(system => {
            console.log(system);
            systems.push(
                <LeCard classNames={`${'system-creation'} ${(this.state.systemSelected && this.state.systemSelected.name) == system.name ? 'selected' : ''}`} clickHandler={() => {
                    // console.log('CLIKC');
                    this.setState({ systemSelected: system }, this.validate);
                }}>
                    <LeCardImg src={system.img} classNames="system-image" />
                    {/* <LeCardBody contentAlignment={CENTER}>
                        <p>{system.text}</p>
                    </LeCardBody> */}
                </LeCard>
            );
        }, this);

        // systems.push(<p>APIs gives us list of systems?</p>);
        return systems;
    }

    render() {
        return (
            <ReactMainContainer>
                <LeVPanel vstretch={"true"} hstretch={"true"} className={"system-creation"}>
                    <p className="le-header">Import Data: Add System</p>
                    <p className="le-sub-header">Select System</p>
                    <GridLayout gap={MIN_GAP} classNames="systems-grid">
                        {this.getSystemSupported()}
                    </GridLayout>

                    <LeHPanel className="system-info">
                        {/* <span className="le-label">Select System</span> */}
                        <LeInputText config={{
                            label: 'System Name'
                        }} callback={
                            (val) => {
                                // console.log('VALUE ', val);
                                this.setState({ newSystemName: val }, this.validate);
                            }
                        }></LeInputText>
                    </LeHPanel>
                    <p className="le-description">The system will automatically create default field mappings between Lattice and Salesforce. The system will automatically create 3 separate field mapping one each for Salesforce Accounts, Contacts & Leads.
                     <br /><br />
                        These field mappings are here to help you get you started quickly and can be edited anytime.
                    </p>
                </LeVPanel>
                <LeToolBar justifycontent={SPACE_BETWEEN}>
                    <LeButton
                        name="cancel"
                        config={{
                            label: "Cancel",
                            classNames: "gray-button"
                        }}
                        callback={() => {
                            ReactRouter.getStateService().go('templateslist');
                            // alert('Call APIS');
                        }}
                    />
                    <LeButton
                        name="create"
                        disabled={!this.state.valid}
                        config={{
                            label: "Create",
                            classNames: "blue-button"
                        }}
                        callback={() => {
                            var observer = new Observer(
                                response => {
                                    httpService.unsubscribeObservable(observer);
                                    ReactRouter.getStateService().go('templateslist');
                                }
                            );
                            // pls/cdl/s3import/system?systemName={systemName}&systemType=
                            // url, body, observer, headers
                            let url = `${'/pls/cdl/s3import/system?systemName='}${this.state.newSystemName}${'&systemType='}${this.state.systemSelected.name}`;
                            httpService.post(url, {}, observer, {});
                            // let config = {
                            //     callback: (action) => {
                            //         modalActions.closeModal(store);
                            //     },
                            //     template: () => {
                            //         return (<p>Leo test</p>)
                            //     },
                            //     title: () => {
                            //         return (<p>TEST</p>);
                            //     },
                            //     oneButton: false,
                            //     hideFooter: false,
                            //     size: MEDIUM_SIZE
                            // }
                            // modalActions.error(store, config);
                        }}
                    />
                </LeToolBar>
            </ReactMainContainer>
        );
    }
}
