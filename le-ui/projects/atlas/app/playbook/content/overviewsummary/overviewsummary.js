import React, {
    Component,
    react2angular
} from "../../../../../common/react-vendor";
import { moment } from "common/vendor.index"
import "./overviewsummary.scss";
import "../../../../../common/assets/sass/mixins.scss";
import "../../../../../common/assets/sass/_tooltips.scss";
import Aux from "../../../../../common/widgets/hoc/_Aux";
import SummaryBox from "./overviewsummary-box";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";
import {
    LEFT,
    RIGHT,
    TOP,
    BOTTOM,
    CENTER,
    SPACEAROUND,
    SPACEBETWEEN,
    SPACEEVEN
} from "common/widgets/container/le-alignments";

import { store } from 'store';
import { actions } from '../../playbook.redux';

export default class OverviewSummaryContainer extends Component {
    constructor(props) {
        super(props);
        this.state = {
            saving: false,
            loading: false,
            play: null,
            types: null,
            showType: false,
            editing: {}
        };
    }

    componentWillUnmount() {
    }

    componentDidMount() {
        let playstore = store.getState()['playbook'];
        this.state.play = playstore.play;
        this.state.loading = playstore.loading;

        if(!playstore.types) {
            let vm = this;
            actions.fetchTypes(function() {
                let playstore = store.getState()['playbook'];
                vm.state.types = playstore.types;
                vm.setState(vm.state);
            });
        } else {
            this.state.types = playstore.types;
        }
        this.setState(this.state);
    }

    makeSummaryBoxes(summaries) {
        var boxes = [];
        for(var i in summaries) {
            var name = i,
                body = summaries[i];
            if(body) {
                boxes.push(
                    <SummaryBox
                        name={name}
                        body={body}
                    />
                );
            }
        }
        return boxes;
    }

    savePlay(play, opts, cb) {
        this.state.saving = true;
        this.setState(this.state);

        for(var i in opts) {
            if(opts[i] === play[i]) {
                delete opts[i];
            }
        }
        if(!Object.keys(opts).length) { // no changes were made
            this.state.saving = false;
            this.setState(this.state);
            return false;
        }

        var savePlay = Object.assign(opts, {
            name: play.name,
            createdBy: play.createdBy
        });

        var vm = this;
        actions.savePlay(savePlay, function(data) {
            let playstore = store.getState()['playbook'];
            vm.state.play = playstore.play;
            vm.state.saving = false;
            vm.setState(vm.state);
            if(cb && typeof cb === 'function') {
                cb(data);
            }
        });
    }

    makeTypeOptions(play, types) {
        let options = [],
            _types = {},
            delay = 300,
            vm = this;


        types.forEach(function(type) {
            _types[type.displayName] = type;
            var selected = false;
            if(type.displayName === play.playType.displayName) {
                selected = true;
            }
            options.push(
                <li className={`${selected ? 'selected' : ''}`} onClick={() => {save(type)} }>
                    {type.displayName}
                </li>
            );
        });

        let toggle = () => {
                this.state.showType = !this.state.showType;
                this.setState(this.state);
            },
            save = (type) => {
                this.savePlay(play, {playType: type}, function(data) {
                    vm.state.showType = false;
                    vm.setState(vm.state);
                });
            }

        return (
            <div class="types">
                <h4 onClick={toggle}>
                    {play.playType.displayName} 
                    <i class="ico caret-down"></i>
                </h4>
                <ul className={`${this.state.showType ? 'show-type' : ''}`} onMouseLeave={() => {
                    setTimeout(function() {
                        vm.state.showType = false;
                        vm.setState(vm.state);
                    }, delay);
                }}>
                    {options}
                </ul>
            </div>
        );
    }

    constrainText(event, limit, debug) {
        var allowedKeys = [
                8, //backspace
                46, //delete
                37, // left
                39, //right
            ],
            debug = debug || false;

        if(event.target.innerText && limit) {
            let disallow = (event.target.innerText.length >= limit && allowedKeys.indexOf(event.which) === -1); // too long && key is not one in the allowed lists
            if(debug) {
                console.log({
                    length: event.target.innerText.length,
                    keycode: event.which, 
                    indexOf: allowedKeys.indexOf(event.which),
                    if: disallow
                });
            }
            if(disallow) {
                event.preventDefault();
            }
        }
    }

    render() {
        if(this.state.play && this.state.types) {
            let play = this.state.play,
                types = this.state.types,
                defaultDescriptionText = (!this.state.editing.description && !play.description ? 'Add a description' : '');

                if(this.state.saving && !defaultDescriptionText) {
                    defaultDescriptionText = '';
                }
            return (
                <Aux>
                    <div className={'overview-summary'}>
                        <LeVPanel hstretch={"true"} className={'le-summary-container'}>
                            <LeHPanel hstretch={"true"} valignment={CENTER} className={'le-summary-header'}>
                                <p className="title">
                                    <LeVPanel hstretch={"true"} className={'title-container'}>
                                        <div className={'play-name'}>
                                            <div className={'play-type'}>
                                                {this.makeTypeOptions(play, types)}
                                            </div>
                                            <h1>
                                                <span contenteditable="true" 
                                                    onBlur={ (e) => { this.savePlay(play, {displayName: e.target.innerText}) } } 
                                                    onKeyDown={(event) => { this.constrainText(event, 255)} } tabIndex={0}>
                                                    {play.displayName}
                                                </span>
                                            </h1>
                                            <div className={'play-created'}>
                                                <LeHPanel hstretch={"true"} valignment={CENTER} className={'le-summary-times'}>
                                                    <span className={'right-border'}>
                                                        Created: {moment(play.created).format('MMM D, YYYY')} 
                                                        <i className={'user-ico show-tooltip top'}>
                                                            <div className={'tooltip_'}>
                                                                <div className={'cover'}>
                                                                    by {play.createdBy}
                                                                </div>
                                                            </div>
                                                        </i>
                                                    </span>
                                                    <span>
                                                        Edited: {moment(play.updated).format('MMM D, YYYY')}
                                                        <i className={'user-ico show-tooltip top'}>
                                                            <div className={'tooltip_'}>
                                                                <div className={'cover'}>
                                                                    by {play.updatedBy}
                                                                </div>
                                                            </div>
                                                        </i>
                                                    </span>
                                                </LeHPanel>
                                            </div>
                                        </div>
                                    </LeVPanel>
                                </p>
                                <p className={`description ${!play.description ? 'is-empty' : ''}`}>
                                    <span contenteditable="true" data-default={defaultDescriptionText} 
                                        onClick={(e) => {
                                            e.target.setAttribute('data-default', '');
                                            this.state.editing.description = true;
                                            this.setState(this.state);
                                        }}
                                        onBlur={ (e) => {
                                            e.target.setAttribute('data-default', defaultDescriptionText);
                                            this.savePlay(play, {description: e.target.innerText});
                                            this.state.editing.description = false;
                                            this.setState(this.state);
                                        }} 
                                        onKeyDown={(event) => { this.constrainText(event, 255)} } tabIndex={1}>
                                        {play.description}
                                    </span>
                                </p>
                            </LeHPanel>

                            <div className="le-flex-h-panel boxes-container">
                                {this.makeSummaryBoxes({
                                    Segment: play.targetSegment.display_name,
                                    Accounts: play.targetSegment.accounts.toLocaleString(),
                                    Contacts: play.targetSegment.contacts.toLocaleString(),
                                    "Scoring Model": (play.ratingEngine ? play.ratingEngine.displayName : null)
                                })}
                            </div>
                        </LeVPanel>
                    </div>
                </Aux>
            );
        } else {
            return (
                <Aux>
                    <p>Loading...</p>
                </Aux>
            );
        }
    }
}

angular
    .module("le.summary", [])
    .component("leSummaryComponent", react2angular(OverviewSummaryContainer, [], []));
