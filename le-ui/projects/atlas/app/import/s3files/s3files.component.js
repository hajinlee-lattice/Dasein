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

        console.log(props);
        this.state = {
            forceReload: false,
            showEmpty: false,
            showLoading: false,
            enableButton: false,
            selectedItem: null,
            data: []
        };
    }

    handleChange = () => {

        console.log(store.getState()['s3files']);
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
        let path = store.getState()['s3files'].path;
        console.log(path);
        s3actions.fetchS3Files(path);

        this.setState({
            forceReload: false,
            showEmpty: false,
            showLoading: true
        });
    }

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
                                    onClick={(rowsSelected) => {
                                        this.setState({ 
                                            enableButton: true,
                                            selectedItem: rowsSelected[0]
                                        });
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
                                            NgState.getAngularState().go('home.import.entry.producthierarchy', {selectedItem: this.state.selectedItem});
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
