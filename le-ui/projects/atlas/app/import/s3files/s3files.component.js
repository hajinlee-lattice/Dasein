import React, { Component } from "common/react-vendor";
import { store, injectAsyncReducer } from 'store';
import { s3actions, s3reducer } from './s3files.redux';
import ReactRouter from '../../react/router';
import NgState from "atlas/ng-state";

import ReactMainContainer from "atlas/react/react-main-container";
import httpService from "common/app/http/http-service";
import { SUCCESS } from "common/app/http/response";
import Observer from "common/app/http/observer";

import Breadcrumbs from "common/widgets/breadcrumbs/breadcrumbs";
import LeTable from "common/widgets/table/table";
import LeButton from "common/widgets/buttons/le-button";
import './s3files.component.scss';

export default class S3FileList extends Component {

    constructor(props) {
        super(props);

        this.ImportWizardStore = ReactRouter.getRouter().ngservices.ImportWizardStore;

        this.state = {
            forceReload: false,
            showEmpty: false,
            showLoading: false,
            enableButton: false,
            selectedItem: null,
            entity: '',
            data: [],
            path: '',
            angularGoTo: '',
            angularBack: '',
            breadcrumbs: []
        };
    }

    componentWillUnmount() {
        this.unsubscribe();
    }

    componentDidMount() {
        injectAsyncReducer(store, 's3files', s3reducer);
        this.unsubscribe = store.subscribe(this.handleChange);
        let path = store.getState()['s3files'].path;

        let ImportWizardStore = this.ImportWizardStore;
        let entityType = ImportWizardStore.getEntityType();
        this.setState({
            showLoading: true,
            breadcrumbs: [
                {
                    label: `${entityType} Data`
                }
            ]
        });
        s3actions.fetchS3Files(path);
    }

    handleChange = () => {
        const data = store.getState()['s3files'];
        let s3Files = data.s3Files;

        let state = Object.assign({}, this.state);
        state.showEmpty = s3Files && s3Files.length == 0;
        state.showLoading = false;
        state.data = s3Files;
        state.path = data.path;
        state.forceReload = true;

        let ImportWizardStore = this.ImportWizardStore;
        let templateData = ImportWizardStore.getTemplateData();
        let templateType = templateData.Object;
        
        let angularGoTo = '';
        let angularBack = '';
        switch (templateType) {
            case "Accounts": {
                angularGoTo = 'home.import.data.accounts.ids';
                angularBack = 'home.import.entry.accounts';
                break;
            }
            case "Contacts": {
                angularGoTo = 'home.import.data.contacts.ids';
                angularBack = 'home.import.entry.contacts';
                break;
            }
            case "Product Purchases": {
                angularGoTo = 'home.import.data.productpurchases.ids';
                angularBack = 'home.import.entry.productpurchases';
                break;
            }
            case "Product Bundles": {
                angularGoTo = 'home.import.data.productbundles.ids';
                angularBack = 'home.import.entry.productbundles';
                break;
            }
            case "Product Hierarchy": {
                angularGoTo = 'home.import.data.producthierarchy.ids';
                angularBack = 'home.import.entry.producthierarchy';
                break;
            }
        }

        state.angularBack = angularBack;
        state.angularGoTo = angularGoTo;
        this.setState(state);
        this.setState({ forceReload: false });
    }

    nextStep = () => {

        // Get feedtype from selection on template list (AccountSchema, ContactSchema, etc.)
        let ImportWizardStore = this.ImportWizardStore;
        let entityType = ImportWizardStore.getEntityType();
        let action = ImportWizardStore.getTemplateAction();

        // Import from S3 file into our system
        let postBody = this.state.selectedItem;
        httpService.post(
            "/pls/models/uploadfile/importFile?entity=" + entityType,
            postBody,
            new Observer(
                response => {
                    if (response.getStatus() === SUCCESS) {

                        switch (action) {
                            case "create-template": 
                            case "edit-template": {
                                console.log(response);
                                let sourceFile = response.data.Result;
                                ImportWizardStore.setCsvFileName(sourceFile.name);
                                NgState.getAngularState().go(this.state.angularGoTo, {});
                                break;
                            }
                            case "import-data": {
                                NgState.getAngularState().go('home.importtemplates', {});
                                break;
                            }
                        }
                    }
                },
                error => {
                    console.log("error");
                }
            )
        );
    }

    goBack = () => {
        console.log(this.state.angularBack);
        NgState.getAngularState().go(this.state.angularBack, {});
    }

    selectFile = (fileObj) => {
        let file = Object.values(fileObj)[0];
        if (file.is_directory) {
            this.getFilesFromFolder(file.file_name);
        } else {


            console.log(file);
            let state = Object.assign({}, this.state);
            state.selectedItem = file;
            // state.entity = ;
            state.enableButton = true;
            this.setState(state);
        }
    }

    getFilesFromFolder = (folder) => {
        let ImportWizardStore = this.ImportWizardStore;
        let templateData = ImportWizardStore.getTemplateData();
        let templateType = templateData.Object;

        let state = Object.assign({}, this.state);
        state.breadcrumbs = [
            {
                "label": `${templateType} Data`
            },
            {
                "label": folder
            }
        ];

        if (typeof folder == 'object'){
            let folderLabel = folder.label;
            let path = this.state.path;

            s3actions.fetchS3Files(path);
            state.breadcrumbs = [
                {
                    "label": `${templateType} Data`
                }
            ];
        } else {
            let newPath = this.state.path + folder;
            let folderData = s3actions.fetchS3Files(newPath);
        }
        
        state.forceReload = true;
        this.setState(state);
        this.setState({ forceReload: false });
    }

    getConfig() {
        let config = {
            name: "file-list",
            selectable: true,
            sorting:{
                initial: 'lastModified',
                direction: 'desc'
            },
            header: [
                {
                    name: "fileName",
                    displayName: "File",
                    sortable: true
                },
                {
                    name: "file_type",
                    displayName: "File Type",
                    sortable: false
                },
                {
                    name: "file_size",
                    displayName: "File Size",
                    sortable: true
                },
                {
                    name: "lastModified",
                    displayName: "Time",
                    sortable: true
                }
            ],
            columns: [
                {
                    colSpan: 7,
                    template: cell => {
                        if (cell.props.rowData.file_type != null) {
                            return (
                                <span>{cell.props.rowData.file_name}</span>
                            );
                        } else {
                            return (
                                <span onClick={() => {
                                    this.getFilesFromFolder(cell.props.rowData.file_name)
                                }}>
                                    <i className="fa fa-folder"></i> {cell.props.rowData.file_name}
                                </span>
                            );
                        }
                    }
                },
                {
                    colSpan: 1
                },
                {
                    colSpan: 1
                },
                {
                    colSpan: 3,
                    template: cell => {

                        if (cell.props.rowData.file_type != null) {
                            let date = new Date(cell.props.rowData.last_modified);
                            let options = {
                                year: "numeric",
                                month: "2-digit",
                                day: "2-digit",
                                hour: "2-digit",
                                minute: "2-digit"
                            };
                            let formatted = date.toLocaleDateString(
                                                "en-US",
                                                options
                                            );
                            return (
                                <span>{formatted}</span>
                            );
                        } else {
                            return null;
                        }
                    }
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
                            <div className="section-header"><h4>Browse S3</h4></div>
                            <hr />
                            <div className="section-body s3files with-padding">

                                <Breadcrumbs 
                                    name="s3files-breadcrumb"
                                    breadcrumbs={this.state.breadcrumbs}
                                    onClick={(crumb) => {
                                        this.getFilesFromFolder(crumb);
                                    }}
                                />

                                <LeTable
                                    name="s3files-table"
                                    config={this.getConfig()}
                                    forceReload={this.state.forceReload}
                                    showLoading={this.state.showLoading}
                                    showEmpty={this.state.showEmpty}
                                    data={this.state.data}
                                    onClick={(rowsSelected) => {
                                        this.selectFile(rowsSelected);
                                    }}
                                />
                            </div>
                            <hr />
                            <div className="container section-actions row form-actions">
                                <div className="pull-left">
                                    <LeButton
                                        name="add"
                                        config={{
                                            label: "Cancel",
                                            classNames: "white-button"
                                        }}
                                        callback={() => {
                                            this.goBack();
                                        }}
                                    />
                                </div>
                                <div className="pull-right">
                                    <LeButton
                                        name="add"
                                        disabled={!this.state.enableButton}
                                        config={{
                                            label: "Select",
                                            classNames: "blue-button"
                                        }}
                                        callback={() => {

                                            this.nextStep();

                                            // Same functionality as Next, Field mappings 
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
