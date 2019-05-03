import React, { Component } from "../../../../common/react-vendor";
import NgState from "../../ng-state";

import ReactMainContainer from "atlas/react/react-main-container";
import httpService from "../../../../common/app/http/http-service";
import { SUCCESS } from "../../../../common/app/http/response";

import { store, injectAsyncReducer } from 'store';
import { s3actions, s3reducer } from 'atlas/import/s3files/s3files.redux';

import TemplatesRowActions, {
    CREATE_TEMPLATE,
    EDIT_TEMPLATE,
    IMPORT_DATA
} from "./templates-row-actions";
import "./templates.scss";
import Observer from "../../../../common/app/http/observer";
import EditControl from "../../../../common/widgets/table/controlls/edit-controls";
import CopyComponent from "../../../../common/widgets/table/controlls/copy-controll";
import EditorText from "../../../../common/widgets/table/editors/editor-text";

import LeButton from "common/widgets/buttons/le-button";
import {
    LeToolBar
} from "common/widgets/toolbar/le-toolbar";
import "./templates.scss";

import messageService from "common/app/utilities/messaging-service";
import Message, {
    NOTIFICATION
} from "common/app/utilities/message";

import LeTable from "common/widgets/table/table";

export default class TemplatesComponent extends Component {
    constructor(props) {
        super(props);

        this.emailCredentialConfig = {
            label: "Setup Automation",
            classNames: "gray-button"
        };
        this.state = {
            forceReload: false,
            showEmpty: false,
            showLoading: false,
            data: []
        };

    }

    createTemplate(response) {
        let entity = "";
        switch (response.type) {
            case "Accounts": {
                entity = "accounts";
                break;
            }
            case "Contacts": {
                entity = "contacts";
                break;
            }
            case "Product Purchases": {
                entity = "productpurchases";
                break;
            }
            case "Product Bundles": {
                entity = "productbundles";
                break;
            }
            case "Product Hierarchy": {
                entity = "producthierarchy";
                break;
            }
        }
        let goTo = `home.import.entry.${entity}`;
        s3actions.setPath(response.data.Path);
        NgState.getAngularState().go(goTo, response);
    }

    actionCallbackHandler = (response) => {
        switch (response.action) {
            case CREATE_TEMPLATE:
                this.createTemplate(response);
                break;
            case EDIT_TEMPLATE:
                this.createTemplate(response);
                break;
            case IMPORT_DATA:
                this.createTemplate(response);
                break;
        }
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
                    name: "TemplateName",
                    displayName: "Name",
                    sortable: false
                },
                {
                    name: "Object",
                    displayName: "Object",
                    sortable: false
                },
                {
                    name: "Path",
                    displayName: "Automated Import Location",
                    sortable: false
                },
                {
                    name: "LastEditedDate",
                    displayName: "Last Modified",
                    sortable: false
                },
                {
                    name: "actions",
                    sortable: false
                }
            ],
            columns: [
                {
                    colspan: 1,
                    template: cell => {

                        let rowData = cell.props.rowData;
                        return (
                            <i 
                                className={"play-pause fa " + (rowData.ImportStatus == "Active" ? 'fa-pause' : 'fa-play')} 
                                aria-hidden="true" 
                                onClick={() => {
                                    this.updateStatus(rowData)
                                }}>
                            </i>
                        );
                    }
                },
                {
                    colSpan: 2,
                    template: cell => {
                        if (!cell.state.saving && !cell.state.editing) {
                            if (cell.props.rowData.Exist) {
                                return (
                                    <EditControl
                                        icon="fa fa-pencil-square-o"
                                        title="Edit Name"
                                        toogleEdit={cell.toogleEdit}
                                        classes="initially-hidden"
                                    />
                                );
                            } else {
                                return null;
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
                    colSpan: 2
                },
                {
                    colSpan: 2,
                    template: cell => {
                        if (cell.props.rowData.Exist) {
                            return (
                                <CopyComponent
                                    title="Copy Link"
                                    data={
                                        cell.props.rowData[cell.props.colName]
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
                            );
                        } else {
                            return null;
                        }
                    }
                },
                {
                    colSpan: 2,
                    mask: value => {
                        var options = {
                            year: "numeric",
                            month: "2-digit",
                            day: "2-digit",
                            hour: "2-digit",
                            minute: "2-digit"
                        };
                        var formatted = new Date(value);
                        // console.log(
                        //     `grid formatted: ${formatted} value: ${value} options: ${options}`
                        // );
                        var buh = "err";
                        try {
                            buh = formatted.toLocaleDateString(
                                "en-US",
                                options
                            );
                        } catch (e) {
                            console.log(e);
                        }

                        return buh;
                    }
                },
                {
                    colSpan: 3,
                    template: cell => {
                        return (
                            <TemplatesRowActions
                                rowData={cell.props.rowData}
                                callback={this.actionCallbackHandler}
                            />
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
                <LeToolBar>
                    <div className="right">
                        <LeButton
                            name="credentials"
                            config={this.emailCredentialConfig}
                            callback={() => {
                                httpService.get(
                                    "/pls/dropbox",
                                    new Observer(response => {
                                        // console.log("BACK HERE ", response);
                                    }),
                                    {
                                        ErrorDisplayMethod: "Banner",
                                        ErrorDisplayOptions: '{"title": "Warning"}',
                                        ErrorDisplayCallback: "TemplatesStore.checkIfRegenerate"
                                    }
                                );
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
    }
}
