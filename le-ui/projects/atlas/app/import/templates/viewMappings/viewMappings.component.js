import React, { Component } from "common/react-vendor";
import { store } from 'store';
import ReactRouter from '../../../react/router';
import NgState from "atlas/ng-state";

import ReactMainContainer from "atlas/react/react-main-container";
import httpService from "common/app/http/http-service";
import { SUCCESS } from "common/app/http/response";
import Observer from "common/app/http/observer";

import LeTable from "common/widgets/table/table";
import LeLink from "common/widgets/link/le-link";
import LeButton from "common/widgets/buttons/le-button";
import './viewMappings.component.scss';
import FeatureFlagsUtilities, { ENABLE_MULTI_TEMPLATE_IMPORT } from 'common/app/services/featureFlags.utilities';

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
            allMappings: [],
            latticeMappings: [],
            customMappings: [],
            downloadData: []
        };
    }

    componentWillUnmount () {
        this.unsubscribe();
    }

    componentDidMount () {
        this.unsubscribe = store.subscribe(this.handleChange);

        let ImportWizardStore = this.ImportWizardStore;
        let postBody = ImportWizardStore.getTemplateData();
        let feedType = {
            FeedType: postBody.FeedType
        };
        let latticeMappings = [];
        let customMappings = [];
        
        httpService.post(
            "/pls/cdl/s3import/template/preview",
            postBody,
            new Observer(
                response => {
                    if (response.getStatus() === SUCCESS) {
                        // state.latticeMappings = respons;
                        let data = response.data;
                        latticeMappings = data.filter(field => field.field_category == "LatticeField");
                        customMappings = data.filter(field => field.field_category == "CustomField");

                        this.setState({
                            forceReload: true,
                            allMappings: data,
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

        httpService.post(
            "/pls/cdl/s3import/template/downloadcsv?source=File",
            feedType,
            new Observer(
                response => {
                    if (response.getStatus() === SUCCESS) {
                        let data = response.data;
                        this.setState({
                            downloadData: data
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

        let state = Object.assign({}, this.state);
        state.forceReload = true;
        state.entity = entity;
        state.object = object
        this.setState(state, function () {
            this.setState({ forceReload: false });    
        });
    }
    
    getLatticeFieldsConfig () {

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
                        if (cell.props.rowData.unmapped) {
                            return (
                                <span className="unmapped">
                                    Unmapped <i className="fa fa-long-arrow-right pull-right" aria-hidden="true"></i>
                                </span>
                            );
                        } else {
                            return (
                                <span>
                                    {cell.props.rowData.name_from_file} <i className="fa fa-long-arrow-right pull-right" aria-hidden="true"></i>
                                </span>
                            );
                        }
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

    getCustomFieldsConfig () {
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
                    displayName: "Custom Field",
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

    getDownloadButton () {
        let ImportWizardStore = this.ImportWizardStore;
        let object = ImportWizardStore.getObject();

        if (object == 'Accounts' || object == 'Contacts') {
            return (
                <LeButton
                    name="downloadMappings"
                    config={{
                        label: "Download Mappings",
                        classNames: "white-button"
                    }}
                    callback={() => {
                        this.downloadMappings()
                    }}
                />
            )
        } else {
            return null;
        }
    }

    downloadMappings () {
        let ImportWizardStore = this.ImportWizardStore;
        let object = ImportWizardStore.getObject();
        let fileName = object.toLowerCase() + "-mappings.csv";
        let downloadData = this.state.downloadData;

        var element = document.createElement("a"); 
        element.download = fileName; 
        var file = new Blob([downloadData], { 
            type: "text/csv;charset=utf-8;" 
        }); 
        var fileURL = window.URL.createObjectURL(file); 
        element.href = fileURL; 
        document.body.appendChild(element); 
        element.click(); 
        document.body.removeChild(element); 
    }

    render() {
        return (
            <ReactMainContainer>
                <section className="container setup-import data-import">
                    <div className="row">
                        <div className="columns twelve box-outline view-mappings">
                            <div className="section-header">
                                <h4>
                                    <LeLink
                                      config={{
                                        label: "< Back",
                                        classes: "back-button",
                                        name: ""
                                      }}
                                      callback={() => {
                                        NgState.getAngularState().go('home.importtemplates', {});
                                      }}
                                    />
                                    View {this.state.object} Mappings
                                </h4>
                            </div>
                            <hr />
                            <div className="section-body with-padding">
                                <h5>
                                    <i className="ico ico-lattice-dots-color"></i> Lattice Fields
                                    {this.getDownloadButton()}
                                </h5>
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
