import React, { Component } from "common/react-vendor";
import { store, injectAsyncReducer } from 'store';
import { actions, reducer } from './s3files.redux';
import ReactRouter from '../../../react/router';
import './s3files.component.scss';
import ReactMainContainer from "atlas/react/react-main-container";

export default class S3FileList extends Component {

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
        actions.fetchTemplates();
        this.setState({
            forceReload: false,
            showEmpty: false,
            showLoading: true
        });
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
            header: [
                {
                    name: "Actions",
                    displayName: "Actions",
                    sortable: false
                },
                {
                    name: "SystemName",
                    displayName: "System Name",
                    sortable: false
                },
                {
                    name: "System",
                    displayName: "System",
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
                    colSpan: 1
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
                    colSpan: 1
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
                        var buh = "err";
                        try {
                            buh = formatted.toLocaleDateString(
                                "en-US",
                                options
                            );
                        } catch (e) {
                            // console.log(e);
                        }

                        return buh;
                    }
                },
                {
                    colSpan: 1,
                    template: cell => {
                        return (
                            <LeHPanel hstretch={'true'} halignment={SPACEEVEN} valignment={CENTER}>
                                <i class="fa fa-upload" aria-hidden="true" onClick={() => {
                                    bannerActions.info(store, {title: 'Test Banner', message: 'Here the message'});
                                }}></i>
                                <i class="fa fa-plus" aria-hidden="true" onClick={() => {
                                    bannerActions.error(store, {title: 'Test Banner 2', message: 'Here the message 1'});
                                }}></i>
                                <i class="fa fa-pencil-square-o" aria-hidden="true" onClick={() => {
                                    bannerActions.success(store, {title: 'Test Banner 3', message: 'Here the message 2'});
                                }}></i>

                            </LeHPanel>
                            // <TemplatesRowActions
                            //     rowData={cell.props.rowData}
                            //     callback={this.actionCallbackHandler}
                            // />
                            // rowData={cell.props.rowData}
                            // callback={this.actionCallbackHandler}
                            // />
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
                <LeToolBar justifycontent={SPACE_BETWEEN}>
                    <p>You can find access tokens to your automation drop folder under connection – S3 – Get Access Tokens</p>
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
