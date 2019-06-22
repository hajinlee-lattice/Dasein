import React, { Component } from 'common/react-vendor';
import LeVPanel from 'common/widgets/container/le-v-panel.js';
import ReactMainContainer from '../../../react/react-main-container';
import ReactRouter from '../../../react/router';
import './match-priority.component.scss';
import LeHPanel from '../../../../../common/widgets/container/le-h-panel';
import LeButton from 'common/widgets/buttons/le-button';
import PrioritiesLitComponent from './priorities-list.component';
import { SPACEBETWEEN } from '../../../../../common/widgets/container/le-alignments';

import { actions, reducer } from './multipletemplates.redux';
import { store, injectAsyncReducer } from 'store';

export default class MatchPriorityComponent extends Component {
    constructor(props) {
        super(props);
        this.updateList = this.updateList.bind(this);
        this.savePriority = this.savePriority.bind(this);
        this.state = {
            items: [],
            loading: true,
            saving: false
        };
    }

    handleChange = () => {
        const data = store.getState()['priorities'];
        // let priorities = data.priorities || [];
        this.setState({
            loading: false,
            items: data.priorities,
        });
        if(this.state.saving && data.saved){
            ReactRouter.getStateService().go(
                'templateslist'
            );
        }
    };
    componentDidMount() {
        injectAsyncReducer(store, 'priorities', reducer);
        this.unsubscribe = store.subscribe(this.handleChange);
        actions.fetchPriorities();
    }

    componentWillUnmount() {
        this.unsubscribe();
        actions.resetPriorities();
    }

    updateList(newList) {
        this.setState({ items: newList });
    }
    savePriority(){
        this.setState({saving: true});
        actions.savePriorities(this.state.items);
    }

    render() {
        return (
            <ReactMainContainer className="match-priority">
                <h2>Updating Match Priority​</h2>
                <LeVPanel
                    hstretch={true}
                    vstretch={true}
                    className="wizard-container"
                >
                    <p className="sub-header">Update Match Priority​</p>

                    <LeHPanel hstretch={'true'}>
                        <PrioritiesLitComponent
                            changeHandler={this.updateList}
                            loading={this.state.loading}
                            saving={this.state.saving}
                            items={this.state.items}
                        />
                        <LeVPanel className="info">
                            <p>MATCH PRIORITY</p>
                            <span>
                                If any of the systems provide multiple IDs,
                                Lattice will use the ID with the highest
                                priority while matching accounts and contacts
                            </span>
                        </LeVPanel>
                    </LeHPanel>
                    <LeHPanel
                        hstretch={true}
                        halignment={SPACEBETWEEN}
                        className="priorities-controlls"
                    >
                        <LeButton
                            name="cancel"
                            config={{
                                label: 'Cancel',
                                classNames: 'gray-button',
                            }}
                            callback={() => {
                                ReactRouter.getStateService().go(
                                    'templateslist'
                                );
                                // alert('Call APIS');
                            }}
                        />
                        <LeButton
                            name="update"
                            disabled={this.state.saving}
                            config={{
                                label: 'Update',
                                classNames: 'blue-button',
                            }}
                            callback={this.savePriority}
                        />
                    </LeHPanel>
                </LeVPanel>
            </ReactMainContainer>
        );
    }
}
