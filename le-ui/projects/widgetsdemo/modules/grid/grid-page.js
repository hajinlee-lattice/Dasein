import React, { Component } from '../../../common/react-vendor';
import Aux from '../../../common/widgets/hoc/_Aux';

import LeMenu from '../../../common/widgets/menu/le-menu'
import LeMenuItem from '../../../common/widgets/menu/le-menu-item';
import LeButton from '../../../common/widgets/buttons/le-button';
import LeTile from '../../../common/widgets/container/tile/le-tile';
import LeTileHeader from '../../../common/widgets/container/tile/le-tile-header';
import LeTileBody from '../../../common/widgets/container/tile/le-tile-body';
import LeTileFooter from '../../../common/widgets/container/tile/le-tile-footer';

import '../../../common/widgets/layout/layout.scss';
import './grid.scss';

export default class GridPage extends Component {
    constructor(props) {
        super(props);
        console.log('Component initialized');
    }

    render() {
        const config = {
            lable: "Test",
            classNames: ['button', 'orange-button']
        }
        const editConfig = {
            lable: "",
            classNames: ['button', 'borderless-button'],
            image: "fa fa-pencil-square-o"
        }
        return (
            <Aux>
                <div className="le-flex-h-panel container-grid">
                    <div className="le-flex-grid">
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
            </Aux>
        );
    }
}