import React, { Component } from "common/react-vendor";
import { store, injectAsyncReducer } from 'store';
import { s3actions, s3reducer } from './s3files.redux';
import ReactRouter from '../../react/router';
import NgState from "atlas/ng-state";

import Breadcrumbs from "common/widgets/breadcrumbs/breadcrumbs";
import LeTable from "common/widgets/table/table";
import LeButton from "common/widgets/buttons/le-button";
import ReactMainContainer from "atlas/react/react-main-container";
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
            data: [],
            path: '',
            angularGoTo: '',
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

        this.setState({
            showLoading: true
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
        state.breadcrumbs = [
            {
                name: 'files',
                label: 'Account Data'
            }
        ]
        state.forceReload = true;

        let ImportWizardStore = this.ImportWizardStore;
        let feedType = ImportWizardStore.getFeedType();
        
        let angularGoTo = '';
        switch (feedType) {
            case "AccountSchema": {
                angularGoTo = 'home.import.data.accounts';
                break;
            }
            case "ContactSchema": {
                angularGoTo = 'home.import.data.contacts';
                break;
            }
            case "TransactionSchema": {
                angularGoTo = 'home.import.data.productpurchases';
                break;
            }
            case "BundleSchema": {
                angularGoTo = 'home.import.data.productbundles';
                break;
            }
            case "HierarchySchema": {
                angularGoTo = 'home.import.data.producthierarchy';
                break;
            }
        }

        state.angularGoTo = angularGoTo;
        this.setState(state);
        this.setState({ forceReload: false });
    }

    selectFile = (fileObj) => {
        let file = Object.values(fileObj)[0];
        if (file.is_directory) {
            this.getFilesFromFolder(file.file_name);
        } else {
            let ImportWizardStore = this.ImportWizardStore;
            ImportWizardStore.setCsvFileName(file.file_name);

            let state = Object.assign({}, this.state);
            state.selectedItem = file;
            state.enableButton = true;
            this.setState(state);
        }
    }

    getFilesFromFolder = (folder) => {
        let newPath = this.state.path + folder;
        let folderData = s3actions.fetchS3Files(newPath);
        // let state = Object.assign({}, this.state);
        // state.data = folderData;
        // this.setState(state);
    }

    backToParentFolder = () => {
        let parentFolderData = s3actions.fetchS3Files(this.state.path);

        this.setState({
            data: parentFolderData
        });
    }

    getConfig() {
        let config = {
            name: "file-list",
            selectable: true,
            header: [
                {
                    name: "fileName",
                    displayName: "File",
                    sortable: false
                },
                {
                    name: "file_type",
                    displayName: "File Type",
                    sortable: false
                },
                {
                    name: "file_size",
                    displayName: "File Size",
                    sortable: false
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
                                    callback={name => {
                                        console.log("NAME ", name);
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
                                            NgState.getAngularState().go('home.importtemplates', {});
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
                                            // Same functionality as Next, Field mappings 
                                            NgState.getAngularState().go(this.state.angularGoTo, {selectedItem: this.state.selectedItem});
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
