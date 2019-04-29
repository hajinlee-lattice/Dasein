import React, { Component } from "../../../../common/react-vendor";
import NgState from "../../ng-state";

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

import messageService from "../../../../common/app/utilities/messaging-service";
import Message, {
    NOTIFICATION
} from "../../../../common/app/utilities/message";

import LeTable from "../../../../common/widgets/table/table";
export default class GridContainer extends Component {
    constructor(props) {
        super(props);
        this.actionCallbackHandler = this.actionCallbackHandler.bind(this);
        this.saveTemplateNameHandler = this.saveTemplateNameHandler.bind(this);
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

        console.log(response.data.Path);
        s3actions.setPath(response.data.Path);

        NgState.getAngularState().go(goTo, response);
    }

    actionCallbackHandler(response) {
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

    saveTemplateNameHandler(cell, value) {
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
    getConfig() {
        let config = {
            name: "import-templates",
            header: [
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
                    colSpan: 3,
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
            <div>
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
            </div>
        );
    }
}
