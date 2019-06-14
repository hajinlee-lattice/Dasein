import React, { Component } from "common/react-vendor";
import { store } from 'store';
import ReactRouter from '../../../react/router';
import NgState from "atlas/ng-state";

import ReactMainContainer from "atlas/react/react-main-container";
import httpService from "common/app/http/http-service";
import { SUCCESS } from "common/app/http/response";
import Observer from "common/app/http/observer";

import LeTable from "common/widgets/table/table";
import LeButton from "common/widgets/buttons/le-button";
import './viewMappings.component.scss';
import FeatureFlagsUtilities, { ENABLE_MULTI_TEMPLATE_IMPORT } from '../../../../../common/app/services/featureFlags.utilities';

export default class ViewMappings extends Component {

    constructor(props) {
        super(props);
        // console.log('STORE ', store.getState());

        this.ImportWizardStore = ReactRouter.getRouter().ngservices.ImportWizardStore;

        this.state = {
            forceReload: false,
            showEmpty: false,
            showLoading: false,
            entity: '',
            object: '',
            latticeMappings: [],
            customMappings: []
        };

        console.log(props);
    }

    componentWillUnmount() {
        this.unsubscribe();
    }

    componentDidMount() {
        this.unsubscribe = store.subscribe(this.handleChange);

        let ImportWizardStore = this.ImportWizardStore;
        let postBody = ImportWizardStore.getTemplateData();
        let latticeMappings = [];
        let customMappings = [];
        httpService.post(
            "/pls/cdl/s3import/template/preview",
            postBody,
            new Observer(
                response => {
                    if (response.getStatus() === SUCCESS) {
                        console.log(response);
                        // state.latticeMappings = response;

                        let data = response.data;
                        latticeMappings = data.filter(field => field.field_category == "LatticeField");
                        customMappings = data.filter(field => field.field_category == "CustomField");

                        console.log(latticeMappings);
                        console.log(customMappings);

                        this.setState({
                            forceReload: true,
                            latticeMappings: latticeMappings,
                            customMappings: customMappings
                        }, function () {
                            this.setState({ forceReload: false }); 
                        });

                    }
                },
                error => {
                    console.log("error");
                }
            )
        );
    }

    handleChange = () => {
        let ImportWizardStore = this.ImportWizardStore;
        let entity = ImportWizardStore.getEntityType();
        let feedType = ImportWizardStore.getFeedType();
        let object = ImportWizardStore.getObject();

        console.log(ImportWizardStore.getTemplateData());

        let state = Object.assign({}, this.state);
        state.forceReload = true;
        state.entity = entity;
        state.object = object
        this.setState(state, function () {
            this.setState({ forceReload: false });    
        });
    }
    
    getLatticeFieldsConfig() {

        let config = {
            name: "lattice-mappings",
            selectable: false,
            header: [
                {
                    name: "nameFromFile",
                    displayName: "Field Name From File",
                    sortable: false
                },
                {
                    name: "name_in_template",
                    displayName: "Lattice Field",
                    sortable: false
                },
                {
                    name: "field_type",
                    displayName: "Data Type",
                    sortable: false
                }
            ],
            columns: [
                {
                    colSpan: 6,
                    template: cell => {
                        return (
                            <span>{cell.props.rowData.name_from_file} <i className="fa fa-long-arrow-right pull-right" aria-hidden="true"></i></span>
                        );
                    }
                },
                {
                    colSpan: 5
                },
                {
                    colSpan: 1
                }
            ]
        };

        return config;
    }

    getCustomFieldsConfig() {
        let config = {
            name: "custom-mappings",
            selectable: false,
            header: [
                {
                    name: "nameFromFile",
                    displayName: "Field Name From File",
                    sortable: false
                },
                {
                    name: "name_in_template",
                    displayName: "Lattice Field",
                    sortable: false
                },
                {
                    name: "field_type",
                    displayName: "Data Type",
                    sortable: false
                }
            ],
            columns: [
                {
                    colSpan: 6,
                    template: cell => {
                        return (
                            <span>{cell.props.rowData.name_from_file} <i className="fa fa-long-arrow-right pull-right" aria-hidden="true"></i></span>
                        );
                    }
                },
                {
                    colSpan: 5
                },
                {
                    colSpan: 1
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
                            <div className="section-header"><h4>View {this.state.object} Mappings</h4></div>
                            <hr />
                            <div className="section-body view-mappings with-padding">
                                <h5><i className="ico ico-lattice-dots-color"></i> Lattice Fields</h5>
                                <LeTable
                                    name="lattice-mappings"
                                    config={this.getLatticeFieldsConfig()}
                                    forceReload={this.state.forceReload}
                                    showLoading={this.state.showLoading}
                                    showEmpty={this.state.showEmpty}
                                    data={this.state.latticeMappings}
                                />

                                <h5><i className="fa fa-cog"></i> Custom Fields</h5>
                                <LeTable
                                    name="custom-mappings"
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
                                            
                                            // console.log('VIEW STATE ', FeatureFlagsUtilities.isFeatureFlagEnabled(ENABLE_MULTI_TEMPLATE_IMPORT));
                                            if(FeatureFlagsUtilities.isFeatureFlagEnabled(ENABLE_MULTI_TEMPLATE_IMPORT)){
                                                NgState.getAngularState().go('home.multipletemplates', {});
                                            }else {
                                                NgState.getAngularState().go('home.importtemplates', {});
                                            }
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
