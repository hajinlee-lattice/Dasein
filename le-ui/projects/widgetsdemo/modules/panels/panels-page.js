import React, { Component } from '../../../common/react-vendor';
import '../../../common/widgets/layout/layout.scss'
import './panel-page.scss';
import Aux from '../../../common/widgets/hoc/_Aux';

import LeMenu from '../../../common/widgets/menu/le-menu'
import LeMenuItem from '../../../common/widgets/menu/le-menu-item';
import LeButton from '../../../common/widgets/buttons/le-button';
import LeTile from '../../../common/widgets/container/tile/le-tile';
import LeTileHeader from '../../../common/widgets/container/tile/le-tile-header';
import LeTileBody from '../../../common/widgets/container/tile/le-tile-body';
import LeTileFooter from '../../../common/widgets/container/tile/le-tile-footer';

export default class PanelsPage extends Component {

    render() {
        const editConfig = {
            lable: "",
            classNames: ['button', 'borderless-button'],
            image: "fa fa-pencil-square-o"
        }
        return (
            <Aux>
                <div className="le-flex-v-panel center-h main-p">
                    <div className="le-flex-h-panel center-v">
                        <div className="test">Test1 </div>
                        <div className="test">Test2 </div>
                    </div>
                    <div className="le-flex-h-panel main-panel" >

                        <ul className="le-flex-h-panel flex-content center">
                            <li>This is OK</li>
                            <li>This ia a button</li>
                            <li>This ia a nother button</li>
                            <li>Test1</li>
                        </ul>

                        <div className="le-flex-h-panel center test"><div><button>Test1</button></div></div>
                    </div>
                    <div className="le-flex-v-panel center-h grid-list">
                        <div className="grid-3-col">
                            <LeTile>
                                <LeTileHeader>
                                    <span className="le-tile-title">Test 1234</span>
                                    <LeMenu classNames="personalMenu" image="fa fa-ellipsis-v" name="main">
                                        <LeMenuItem
                                            name="edit"
                                            label="Edit"
                                            image="fa fa-pencil-square-o"
                                            callback={(name) => {
                                                console.log('NAME ', name);
                                            }} >
                                        </LeMenuItem>

                                        <LeMenuItem
                                            name="duplicate"
                                            label="Duplicate"
                                            image="fa fa-files-o"
                                            callback={(name) => {
                                                console.log('NAME ', name);
                                            }} />

                                        <LeMenuItem
                                            name="delete"
                                            label="Delete"
                                            image="fa fa-trash-o"
                                            callback={(name) => {
                                                console.log('NAME ', name);
                                            }} />
                                    </LeMenu>
                                </LeTileHeader>
                                <LeTileBody>
                                    <p>The description here</p>
                                    <span>The body</span>
                                </LeTileBody>
                                <LeTileFooter>
                                    <div className="le-flex-v-panel fill-space">
                                        <span>Account</span>
                                        <span>123</span>
                                    </div>
                                    <div className="le-flex-v-panel fill-space">
                                        <span>Contacts</span>
                                        <span>3</span>
                                    </div>
                                </LeTileFooter>
                            </LeTile>

                            <LeTile>
                                <LeTileHeader>
                                    <span className="">Test 1234</span>
                                    <LeMenu classNames="personalMenu" image="fa fa-ellipsis-v" name="main">
                                        <LeMenuItem
                                            name="edit"
                                            label="Edit"
                                            image="fa fa-pencil-square-o"
                                            callback={(name) => {
                                                console.log('NAME ', name);
                                            }} >
                                        </LeMenuItem>

                                        <LeMenuItem
                                            name="duplicate"
                                            label="Duplicate"
                                            image="fa fa-files-o"
                                            callback={(name) => {
                                                console.log('NAME ', name);
                                            }} />

                                        <LeMenuItem
                                            name="delete"
                                            label="Delete"
                                            image="fa fa-trash-o"
                                            callback={(name) => {
                                                console.log('NAME ', name);
                                            }} />
                                    </LeMenu>
                                </LeTileHeader>
                                <LeTileBody>
                                    <p>The description here</p>
                                    <span>The body</span>
                                </LeTileBody>
                                <LeTileFooter>
                                    <div className="le-flex-v-panel fill-space">
                                        <span>Account</span>
                                        <span>123</span>
                                    </div>
                                    <div className="le-flex-v-panel fill-space">
                                        <span>Contacts</span>
                                        <span>3</span>
                                    </div>
                                </LeTileFooter>
                            </LeTile>

                            <LeTile>
                                <LeTileHeader>
                                    <span className="">Test 1234</span>
                                    <LeButton lable="Click" config={editConfig} callback={() => { alert('Test'); }} />
                                </LeTileHeader>
                                <LeTileBody>
                                    <p>The description here</p>
                                    <span>The body</span>
                                </LeTileBody>
                                <LeTileFooter>
                                    <div className="le-flex-v-panel fill-space">
                                        <span>Account</span>
                                        <span>123</span>
                                    </div>
                                    <div className="le-flex-v-panel fill-space">
                                        <span>Contacts</span>
                                        <span>3</span>
                                    </div>
                                </LeTileFooter>
                            </LeTile>

                            <LeTile>
                                <LeTileHeader>
                                    <span className="">Test 1234</span>
                                    <LeButton lable="Click" config={editConfig} callback={() => { alert('Test'); }} />
                                </LeTileHeader>
                                <LeTileBody>
                                    <p>The description here</p>
                                    <span>The body</span>
                                </LeTileBody>
                                <LeTileFooter>
                                    <div className="le-flex-v-panel fill-space">
                                        <span>Account</span>
                                        <span>123</span>
                                    </div>
                                    <div className="le-flex-v-panel fill-space">
                                        <span>Contacts</span>
                                        <span>3</span>
                                    </div>
                                </LeTileFooter>
                            </LeTile>

                        </div>
                    </div>
                </div>

            </Aux>
        );
    }
}