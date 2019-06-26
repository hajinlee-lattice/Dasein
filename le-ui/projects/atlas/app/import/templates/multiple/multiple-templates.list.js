import React, { Component } from "common/react-vendor";
import { store, injectAsyncReducer } from 'store';
import { actions, reducer } from './multipletemplates.redux';
import NgState from "atlas/ng-state";
import { s3actions } from 'atlas/import/s3files/s3files.redux';
import ReactRouter from '../../../react/router';
import TemplatesRowActions, {
    CREATE_TEMPLATE,
    EDIT_TEMPLATE,
    IMPORT_DATA
} from "../templates-row-actions";
import CopyComponent from "common/widgets/table/controlls/copy-controll";
import EditorText from "common/widgets/table/editors/editor-text";

import messageService from "common/app/utilities/messaging-service";
import Message, {
    NOTIFICATION
} from "common/app/utilities/message";

import  LeHPanel from '../../../../../common/widgets/container/le-h-panel';
import LeTable from "common/widgets/table/table";
import './multiple-templates.list.scss';
import LeButton, {RIGHT} from "common/widgets/buttons/le-button";
import ReactMainContainer from "atlas/react/react-main-container";
import { LeToolBar, SPACE_BETWEEN } from "common/widgets/toolbar/le-toolbar";

export default class MultipleTemplatesList extends Component {

    constructor(props) {
        super(props);
        this.ImportWizardStore = ReactRouter.getRouter().ngservices.ImportWizardStore;
        this.TemplatesStore = ReactRouter.getRouter().ngservices.TemplatesStore;

        this.actionCallbackHandler = this.actionCallbackHandler.bind(this);
        this.saveTemplateNameHandler = this.saveTemplateNameHandler.bind(this);
        this.state = {
            forceReload: false,
            showEmpty: false,
            showLoading: false,
            data: []
        };
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

    handleChange = () => {

        const data = store.getState()['multitemplates'];
        let templates = data.templates;
        this.setState({
            forceReload: true,
            showEmpty: templates && templates.length == 0,
            showLoading: false,
            data: templates
        });
        this.setState({ forceReload: false });
    }

    componentWillUnmount() {
        this.unsubscribe();
    }

    componentDidMount() {
        injectAsyncReducer(store, 'multitemplates', reducer);
        this.unsubscribe = store.subscribe(this.handleChange);
        this.setState({
            forceReload: false,
            showEmpty: false,
            showLoading: true
        });
        actions.fetchTemplates();
    }

    saveTemplateNameHandler(cell, value) {
        if (value && value != "") {
            cell.setSavingState();
            let copy = Object.assign({}, this.state.data[cell.props.rowIndex]);
            copy[cell.props.colName] = value;
        }
    }
    getConfig() {
        let config = {
            name: "import-templates",
            selectable: false,
            sorting:{
                initial: 'none',
                direction: 'none'
            },
            header: [
                {
                    name: "Active",
                    displayName: "Active",
                    sortable: false
                },
                {
                    name: "ImportSystem.priority",
                    displayName: "Priority",
                    sortable: false
                },
                {
                    name: "SystemName",
                    displayName: "System Name",
                    sortable: false
                },
                {
                    name: "ImportSystem.system_type",
                    displayName: "System",
                    sortable: true
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
                }
            ],
            columns: [
                {
                    colSpan: 1,
                    onlyTemplate: true,
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
                    colSpan: 1
                },
                {
                    colSpan: 2,
                    onlyTemplate: true,
                    template: cell => {
                        if (!cell.state.saving && !cell.state.editing) {
                            return (<span>{cell.props.rowData.ImportSystem.display_name}</span>);
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
                    colSpan: 1
                },
                {
                    colSpan: 1
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
                    colSpan: 3,
                    onlyTemplate: true,
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
                                {/* {lastEditedDate} */}
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

    setDataTypes = (response) => {
        let state = Object.assign({}, this.state);

        console.log(response.type);
        switch (response.type) {
            case "Accounts": {
                state.entity = "accounts";
                state.entityType = 'Account';
                break;
            }
            case "Contacts": {
                state.entity = "contacts";
                state.entityType = 'Contact';
                break;
            }
            case "Product Purchases": {
                state.entity = "productpurchases";
                state.entityType = 'Product';
                break;
            }
            case "Product Bundles": {
                state.entity = "productbundles";
                state.entityType = 'Product';
                break;
            }
            case "Product Hierarchy": {
                state.entity = "producthierarchy";
                state.entityType = 'Product';
                break;
            }
        }

        state.feedType = response.data.FeedType;
        state.object = response.data.Object;

        this.setState(state, function () {
            
            // Wait until setState is completed and do some stuff.
            let action = response.action;
            let data = response.data;
            let ImportWizardStore = this.ImportWizardStore;
            ImportWizardStore.setEntityType(this.state.entityType);
            ImportWizardStore.setFeedType(this.state.feedType);
            ImportWizardStore.setTemplateAction(action);
            ImportWizardStore.setTemplateData(data);
            ImportWizardStore.setObject(this.state.object);
            if (action == 'view-template') {
                this.viewTemplate(response);
            } else {
                this.createTemplate(response);
            }

        });
    }

    createTemplate = (response) => {
        let params = {
            importOnly: (response.action == 'import-data') ? true : false,
            action: response.action,
            data: response.data
        }

        let goTo = `home.import.entry.${this.state.entity}`;
        s3actions.setPath(response.data.Path);
        NgState.getAngularState().go(goTo, params);
    }

    viewTemplate(response) {
        let params = {
            data: response.data
        }
        NgState.getAngularState().go('home.viewmappings', params);
    }
    render() {
        return (
            <ReactMainContainer>
                <LeToolBar justifycontent={SPACE_BETWEEN}>
                    <p>You can find access tokens to your automation drop folder under connection – S3 – Get Access Tokens</p>
                    <LeHPanel className="multitemplates-toolbar">
                        <LeButton
                            name="matchpriority"
                            config={{
                                classNames: "borderless-button",
                                iconside: RIGHT,
                                icon: 'fa fa-list-ol'
                            }}
                            callback={() => {
                                ReactRouter.getStateService().go('matchpriority');
                            }}
                        />
                        <LeButton
                            name="add"
                            config={{
                                label: "Add System",
                                classNames: "blue-button",
                                iconside: RIGHT,
                                icon: 'fa fa-plus-circle'
                            }}
                            callback={() => {
                                ReactRouter.getStateService().go('sistemcreation');
                            }}
                        />
                    </LeHPanel>
                </LeToolBar>
                <LeTable
                    name="multiple-templates"
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
