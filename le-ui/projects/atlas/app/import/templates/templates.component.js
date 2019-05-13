import React, { Component } from "common/react-vendor";
import NgState from "../../ng-state";

import ReactRouter from 'atlas/react/router';

import ReactMainContainer from "atlas/react/react-main-container";
import httpService from "common/app/http/http-service";
import { SUCCESS } from "common/app/http/response";

import { store, injectAsyncReducer } from 'store';
import { s3actions, s3reducer } from 'atlas/import/s3files/s3files.redux';

import TemplatesRowActions, {
    CREATE_TEMPLATE,
    VIEW_TEMPLATE,
    EDIT_TEMPLATE,
    IMPORT_DATA
} from "./templates-row-actions";
import "./templates.scss";
import Observer from "common/app/http/observer";
import EditControl from "common/widgets/table/controlls/edit-controls";
import CopyComponent from "common/widgets/table/controlls/copy-controll";
import EditorText from "common/widgets/table/editors/editor-text";

import LeButton from "common/widgets/buttons/le-button";
import {
    LeToolBar
} from "common/widgets/toolbar/le-toolbar";
import "./templates.scss";

import { actions as modalActions } from 'common/widgets/modal/le-modal.redux';
import { LARGE_SIZE, MEDIUM_SIZE } from "common/widgets/modal/le-modal.utils";
import messageService from "common/app/utilities/messaging-service";
import Message, {
    NOTIFICATION
} from "common/app/utilities/message";

import LeTable from "common/widgets/table/table";

export default class TemplatesComponent extends Component {
    constructor(props) {
        super(props);

        this.ImportWizardStore = ReactRouter.getRouter().ngservices.ImportWizardStore;
        this.TemplatesStore = ReactRouter.getRouter().ngservices.TemplatesStore;

        this.emailCredentialConfig = {
            label: "Setup Automation",
            classNames: "gray-button"
        };
        this.state = {
            forceReload: false,
            showEmpty: false,
            showLoading: false,
            data: [],
            entity: '',
            entityType: '',
            feedType: ''
        };

    }

    setDataTypes = (response) => {
        let state = Object.assign({}, this.state);
        switch (response.type) {
            case "Accounts": {
                state.entity = "accounts";
                state.entityType = 'Account';
                state.feedType = 'AccountSchema';
                break;
            }
            case "Contacts": {
                state.entity = "contacts";
                state.entityType = 'Contact';
                state.feedType = 'ContactSchema';
                break;
            }
            case "Product Purchases": {
                state.entity = "productpurchases";
                state.entityType = 'Product';
                state.feedType = 'TransactionSchema';
                break;
            }
            case "Product Bundles": {
                state.entity = "productbundles";
                state.entityType = 'Product';
                state.feedType = 'BundleSchema';
                break;
            }
            case "Product Hierarchy": {
                state.entity = "producthierarchy";
                state.entityType = 'Product';
                state.feedType = 'HierarchySchema';
                break;
            }
        }

        this.setState(state, function () {
            
            // Wait until setState is completed and do some stuff.
            let action = response.action;
            let ImportWizardStore = this.ImportWizardStore;
            ImportWizardStore.setEntityType(this.state.entityType);
            ImportWizardStore.setFeedType(this.state.feedType);
            ImportWizardStore.setTemplateAction(action);
            if (action == 'view-template') {
                this.viewTemplate();
            } else {
                this.createTemplate(response);
            }

        });
    }

    createTemplate = (response) => {
        let params = {
            importOnly: (response.action == 'import-data') ? true : false,
            action: response.action
        }

        let goTo = `home.import.entry.${this.state.entity}`;
        s3actions.setPath(response.data.Path);
        NgState.getAngularState().go(goTo, params);
    }

    viewTemplate() {
        NgState.getAngularState().go('home.viewmappings', {});
    }

    componentWillUnmount() {
        httpService.unsubscribeObservable(this.observer);
    }

    componentDidMount() {
        injectAsyncReducer(store, 's3files', s3reducer);

        this.setState({
            forceReload: true,
            showEmpty: false,
            showLoading: true
        });
        this.observer = new Observer(
            response => {
                if (response.status == SUCCESS) {
                    this.setState({
                        forceReload: true,
                        showEmpty: response.data && response.data.length == 0,
                        showLoading: false,
                        data: response.data
                    });
                    this.setState({ forceReload: false });
                } else {
                    this.setState({
                        forceReload: false,
                        showEmpty: true,
                        showLoading: false,
                        data: []
                    });
                }
            },
            error => {
                this.setState({
                    forceReload: false,
                    showEmpty: true,
                    showLoading: false,
                    data: []
                });
            }
        );
        httpService.get("/pls/cdl/s3import/template", this.observer);
    }

    saveTemplateNameHandler = (cell, value) => {
        if (value && value != "") {
            cell.setSavingState();
            let copy = Object.assign({}, this.state.data[cell.props.rowIndex]);
            copy[cell.props.colName] = value;
            httpService.put(
                "/pls/cdl/s3/template/displayname",
                copy,
                new Observer(
                    response => {
                        cell.toogleEdit();
                        if (response.getStatus() === SUCCESS) {
                            let newState = [...this.state.data];
                            newState[cell.props.rowIndex][
                                cell.props.colName
                            ] = value;
                            this.setState({ data: newState });
                        }
                    },
                    error => {
                        cell.toogleEdit();
                    }
                )
            );
        }
    }

    updateStatus = (rowData) => {
        let templates = this.state.data;
        let dataItem = templates.find( template => template.FeedType == rowData.FeedType);
        let newStatus = dataItem.ImportStatus == "Active" ? "Pause" : "Active";
        let postBody = {
            ImportStatus: newStatus,
            FeedType: dataItem.FeedType            
        }

        httpService.put(
            "/pls/cdl/s3/template/status?value=source&required=false&defaultValue=file",
            postBody,
            new Observer(
                response => {
                    if (response.getStatus() === SUCCESS) {
                    
                        let newTemplatesState = [...templates];
                        let updatedDataItem = newTemplatesState.find( template => template.FeedType == rowData.FeedType);
                        updatedDataItem.ImportStatus = newStatus;
                        this.setState({ data: newTemplatesState });

                        messageService.sendMessage(
                            new Message(
                                null,
                                NOTIFICATION,
                                "success",
                                "",
                                "Status updated"
                            )
                        );
                    }
                },
                error => {
                    console.log("error");
                }
            )
        );
    }

    getConfig() {
        let config = {
            name: "import-templates",
            header: [
                {
                    name: "Active",
                    displayName: "Active",
                    sortable: false
                },
                {
                    name: "Name",
                    displayName: "Name",
                    sortable: false
                },
                {
                    name: "S3Path",
                    displayName: "S3 Folder",
                    sortable: false
                },
                {
                    name: "LastEdited",
                    displayName: "Last Modified",
                    sortable: false
                }
            ],
            columns: [
                {
                    colspan: 1,
                    template: cell => {

                        let rowData = cell.props.rowData;

                        if (rowData.Exist) {
                            return (
                                <i 
                                    className={"play-pause fa " + (rowData.ImportStatus == "Active" ? 'fa-pause' : 'fa-play')} 
                                    aria-hidden="true" 
                                    onClick={() => {
                                        this.updateStatus(rowData)
                                    }}>
                                </i>
                            );
                        } else {
                            return null;
                        }
                    }
                },
                {
                    colSpan: 2,
                    template: cell => {
                        if (!cell.state.saving && !cell.state.editing) {
                            if (cell.props.rowData.Exist) {
                                return (
                                    <div className={!cell.props.rowData.TemplateName ? 'no-name' : ''}>
                                        {cell.props.rowData.TemplateName ? cell.props.rowData.TemplateName : 'Name is not defined'}
                                        <ul className="unstyled">
                                            <EditControl
                                                icon="fa fa-pencil-square-o"
                                                title="Edit Name"
                                                toogleEdit={cell.toogleEdit}
                                                classes="initially-hidden"
                                            />
                                        </ul>
                                    </div>
                                );
                            } else {
                                return (
                                    <div className={!cell.props.rowData.TemplateName ? 'no-name' : ''}>
                                        {cell.props.rowData.TemplateName ? cell.props.rowData.TemplateName : 'Name is not defined'}
                                    </div>
                                );
                            }
                        }
                        if (cell.state.editing && !cell.state.saving) {
                            if (cell.props.rowData.Exist) {
                                return (
                                    <EditorText
                                        initialValue={
                                            cell.props.rowData.TemplateName
                                        }
                                        cell={cell}
                                        applyChanges={
                                            this.saveTemplateNameHandler
                                        }
                                        cancel={cell.cancelHandler}
                                    />
                                );
                            } else {
                                return null;
                            }
                        }
                    }
                },
                {
                    colSpan: 5,
                    template: cell => {


                        let longRootFolder = cell.props.rowData.Path;
                        let shortRootFolder = '';
                        let copyComponent = null;

                        if (longRootFolder != 'N/A') {
                            let folderArray = longRootFolder.split('/');
                            folderArray.pop();
                            shortRootFolder = "/" + folderArray[folderArray.length - 1];

                            copyComponent = <CopyComponent
                                    title="Copy Link"
                                    data={longRootFolder}
                                    callback={() => {
                                        messageService.sendMessage(
                                            new Message(
                                                null,
                                                NOTIFICATION,
                                                "success",
                                                "",
                                                "Copied to Clipboard"
                                            )
                                        );
                                    }}
                                />;
                        } else {
                            shortRootFolder = "N/A";
                            copyComponent = null;
                        }



                        return (
                            <div>
                                {shortRootFolder}
                                {copyComponent}
                            </div>
                        );
                    }
                },
                {
                    colSpan: 4,
                    template: cell => {

                        let lastEditedDate = '';
                        let lastEditedDateNumeric = null;

                        if (cell.props.rowData.Exist) {
                            lastEditedDateNumeric = cell.props.rowData.LastEditedDate;
                            var options = {
                                year: "numeric",
                                month: "2-digit",
                                day: "2-digit",
                                hour: "2-digit",
                                minute: "2-digit"
                            };
                            var formatted = new Date(lastEditedDateNumeric);
                            var buh = "err";
                            try {
                                buh = formatted.toLocaleDateString(
                                    "en-US",
                                    options
                                );
                            } catch (e) {
                                console.log(e);
                            }
                            lastEditedDate = buh;
                        }

                        return (
                            <div>
                                {lastEditedDate}
                                <TemplatesRowActions
                                    rowData={cell.props.rowData}
                                    callback={this.setDataTypes}
                                />
                            </div>
                        );
                    }
                }
            ]
        };

        return config;
    }

    render() {

        let data = this.state.data;
        if (data.length > 0) {
            
            let longRootFolder = data[0].Path;
            let folderArray = longRootFolder.split('/');
            folderArray.splice(-2);
            let rootFolder = folderArray.join('/');        

            return (
                <ReactMainContainer className="templates">
                    <LeToolBar justifycontent="space-between">
                        <div>
                            S3 Root Folder: {rootFolder}
                            <ul className="unstyled">
                                <CopyComponent
                                    title="Copy Link"
                                    data={
                                        rootFolder
                                    }
                                    callback={() => {
                                        messageService.sendMessage(
                                            new Message(
                                                null,
                                                NOTIFICATION,
                                                "success",
                                                "",
                                                "Copied to Clipboard"
                                            )
                                        );
                                    }}
                                />
                            </ul>
                        </div>
                        <div>
                            <LeButton
                                name="credentials"
                                config={this.emailCredentialConfig}
                                callback={() => {
                                    this.TemplatesStore.newToken();
                                    // httpService.get(
                                    //     "/pls/dropbox",
                                    //     new Observer(response => {
                                    //         // console.log("BACK HERE ", response);
                                    //     }),
                                    //     {
                                    //         ErrorDisplayMethod: "Banner",
                                    //         ErrorDisplayOptions: '{"title": "Warning"}',
                                    //         ErrorDisplayCallback: "TemplatesStore.checkIfRegenerate"
                                    //     }
                                    // );
                                }}
                            />
                        </div>
                    </LeToolBar>
                    <LeTable
                        name="import-templates"
                        config={this.getConfig()}
                        forceReload={this.state.forceReload}
                        showLoading={this.state.showLoading}
                        showEmpty={this.state.showEmpty}
                        data={this.state.data}
                    />
                    <p>
                        *Atlas currently only supports one template for each object.{" "}
                    </p>
                </ReactMainContainer>
            );
        } else {
            return null;
        }
    }
}
