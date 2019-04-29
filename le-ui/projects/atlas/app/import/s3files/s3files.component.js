import React, { Component } from "common/react-vendor";
import { store, injectAsyncReducer } from 'store';
import { s3actions, s3reducer } from './s3files.redux';
import ReactRouter from '../../react/router';
import NgState from "atlas/ng-state";

import LeTable from "common/widgets/table/table";
import LeButton from "common/widgets/buttons/le-button";
import ReactMainContainer from "atlas/react/react-main-container";
import './s3files.component.scss';

export default class S3FileList extends Component {

    constructor(props) {
        super(props);
        // this.actionCallbackHandler = this.actionCallbackHandler.bind(this);
        // this.saveTemplateNameHandler = this.saveTemplateNameHandler.bind(this);
        this.state = {
            forceReload: false,
            showEmpty: false,
            showLoading: false,
            data: []
        };
    }

    // actionCallbackHandler(response) {
    //     switch (response.action) {
    //         case CREATE_TEMPLATE:
    //             this.createTemplate(response);
    //             break;
    //         case EDIT_TEMPLATE:
    //             this.createTemplate(response);
    //             break;
    //         case IMPORT_DATA:
    //             this.createTemplate(response);
    //             break;
    //     }
    // }

    handleChange = () => {

        const data = store.getState()['s3files'];
        let s3Files = data.s3Files;
        this.setState({
            forceReload: true,
            showEmpty: s3Files && s3Files.length == 0,
            showLoading: false,
            data: s3Files
        });
        this.setState({ forceReload: false });
    }

    componentWillUnmount() {
        this.unsubscribe();
    }

    componentDidMount() {
        injectAsyncReducer(store, 's3files', s3reducer);
        this.unsubscribe = store.subscribe(this.handleChange);
        
        s3actions.fetchS3Files();
        console.log(s3actions.fetchS3Files());

        this.setState({
            forceReload: false,
            showEmpty: false,
            showLoading: true
        });
    }

    // saveTemplateNameHandler(cell, value) {
    //     if (value && value != "") {
    //         cell.setSavingState();
    //         let copy = Object.assign({}, this.state.data[cell.props.rowIndex]);
    //         copy[cell.props.colName] = value;
    //     }
    // }
    

    getConfig() {
        let config = {
            name: "file-list",
            selectable: true,
            header: [
                {
                    name: "file_name",
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
                    name: "last_modified",
                    displayName: "Time",
                    sortable: true
                }
            ],
            columns: [
                {
                    colSpan: 6
                },
                {
                    colSpan: 2
                },
                {
                    colSpan: 2
                },
                {
                    colSpan: 2
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
                        <div className="columns eight offset-two box-outline">
                            <div className="section-header"><h4>Browse S3</h4></div>
                            <hr />
                            <div className="section-body with-padding">
                                <h5>Account Data</h5>
                                <LeTable
                                    name="s3files"
                                    config={this.getConfig()}
                                    forceReload={this.state.forceReload}
                                    showLoading={this.state.showLoading}
                                    showEmpty={this.state.showEmpty}
                                    data={this.state.data}
                                    rowClick={(rowsSelected, rowIndex) => {
                                        console.log('ROW')
                                        console.log(rowsSelected, rowIndex);
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
                                            ReactRouter.getStateService().go('templateslist');
                                        }}
                                    />
                                </div>
                                <div className="pull-right">
                                    <LeButton
                                        name="add"
                                        config={{
                                            label: "Select",
                                            classNames: "blue-button"
                                        }}
                                        callback={() => {
                                            NgState.getAngularState().go('home.import.entry', response);
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
