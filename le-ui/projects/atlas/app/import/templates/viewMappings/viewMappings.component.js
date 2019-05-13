import React, { Component } from "common/react-vendor";
import { store, injectAsyncReducer } from 'store';
import ReactRouter from '../../../react/router';
import NgState from "atlas/ng-state";

import ReactMainContainer from "atlas/react/react-main-container";
import httpService from "common/app/http/http-service";
import { SUCCESS } from "common/app/http/response";
import Observer from "common/app/http/observer";

import LeTable from "common/widgets/table/table";
import LeButton from "common/widgets/buttons/le-button";
import './viewMappings.component.scss';

export default class ViewMappings extends Component {

    constructor(props) {
        super(props);

        this.ImportWizardStore = ReactRouter.getRouter().ngservices.ImportWizardStore;

        this.state = {
            forceReload: false,
            showEmpty: false,
            showLoading: false,
            entity: '',
            latticeMappings: [],
            customMappings: []
        };
    }

    componentWillUnmount() {
        this.unsubscribe();
    }

    componentDidMount() {
        this.unsubscribe = store.subscribe(this.handleChange);
    }

    handleChange = () => {
        let ImportWizardStore = this.ImportWizardStore;
        let entity = ImportWizardStore.getEntityType();
        let feedType = ImportWizardStore.getFeedType();

        let state = Object.assign({}, this.state);
        state.forceReload = true;
        state.entity = entity;

        let postBody = {};

        httpService.post(
            "/pls/cdl/s3import/template/preview?feedType=" + feedType,
            postBody,
            new Observer(
                response => {
                    if (response.getStatus() === SUCCESS) {
                        console.log(response);
                        state.latticeMappings = response;
                    }
                },
                error => {
                    console.log("error");
                }
            )
        );

        // state.latticeMappings = [
        //     {
        //         "fileName": "yo",
        //         "latticeField": "homey",
        //         "dataType": "String"
        //     },
        //     {
        //         "fileName": "yo",
        //         "latticeField": "homey",
        //         "dataType": "String"
        //     }
        // ];
        state.customMappings = [
            {
                "fileName": "yo",
                "latticeField": "homey",
                "dataType": "String"
            },
            {
                "fileName": "yo",
                "latticeField": "homey",
                "dataType": "String"
            }
        ];
        this.setState(state, function () {
            this.setState({ forceReload: false });    
        });
    }
    
    getLatticeFieldsConfig() {
        let config = {
            name: "lattice-fields",
            selectable: false,
            header: [
                {
                    name: "fileName",
                    displayName: "Field Name From File",
                    sortable: false
                },
                {
                    name: "latticeField",
                    displayName: "Lattice Field",
                    sortable: false
                },
                {
                    name: "dataType",
                    displayName: "Data Type",
                    sortable: false
                }
            ],
            columns: [
                {
                    colSpan: 4
                },
                {
                    colSpan: 4
                },
                {
                    colSpan: 4
                }
            ]
        };

        return config;
    }

    getCustomFieldsConfig() {
        let config = {
            name: "custom-fields",
            selectable: false,
            header: [
                {
                    name: "fileName",
                    displayName: "Field Name From File",
                    sortable: false
                },
                {
                    name: "latticeField",
                    displayName: "Lattice Field",
                    sortable: false
                },
                {
                    name: "dataType",
                    displayName: "Data Type",
                    sortable: false
                }
            ],
            columns: [
                {
                    colSpan: 4
                },
                {
                    colSpan: 4
                },
                {
                    colSpan: 4
                }
            ]
        };

        return config;
    }

    render() {
        return (
            <ReactMainContainer>
                <section className="container setup-import data-import">
                    <div className="row">
                        <div className="columns twelve box-outline">
                            <div className="section-header"><h4>View {this.state.entity} Mappings</h4></div>
                            <hr />
                            <div className="section-body view-mappings with-padding">
                                <h5>Lattice Fields</h5>
                                <LeTable
                                    name="lattice-fields"
                                    config={this.getLatticeFieldsConfig()}
                                    forceReload={this.state.forceReload}
                                    showLoading={this.state.showLoading}
                                    showEmpty={this.state.showEmpty}
                                    data={this.state.latticeMappings}
                                />

                                <h5>Custom Fields</h5>
                                <LeTable
                                    name="custom-fields"
                                    config={this.getCustomFieldsConfig()}
                                    forceReload={this.state.forceReload}
                                    showLoading={this.state.showLoading}
                                    showEmpty={this.state.showEmpty}
                                    data={this.state.customMappings}
                                />
                            </div>
                            <hr />
                            <div className="container section-actions row form-actions">
                                <div className="pull-right">
                                    <LeButton
                                        name="done"
                                        config={{
                                            label: "Done",
                                            classNames: "white-button"
                                        }}
                                        callback={() => {
                                            NgState.getAngularState().go('home.importtemplates', {});
                                        }}
                                    />
                                </div>
                            </div>
                        </div>
                    </div>
                </section>
            </ReactMainContainer>
        );
    }
}
