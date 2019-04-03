import React from "common/react-vendor";
import { storiesOf } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import Aux from '../../../common/widgets/hoc/_Aux';
import {
    withKnobs,
    text,
    boolean,
    select,
    number,
    radios
} from "@storybook/addon-knobs";
import "common/assets/css/font-awesome.min.css";
import "common/assets/css/font-awesome.min.css";
import "common/widgets/layout/layout.scss";
import LeTile from 'common/widgets/container/tile/le-tile';
import LeTileHeader from 'common/widgets/container/tile/le-tile-header';
import LeTileBody from 'common/widgets/container/tile/le-tile-body';
import LeTileFooter from 'common/widgets/container/tile/le-tile-footer';
import LeMenu from 'common/widgets/menu/le-menu';
import LeMenuItem from 'common/widgets/menu/le-menu-item';
import GridLayout from 'common/widgets/container/grid-layout.component';


import "../../../common/widgets/layout/le-layouts.scss";
import { MIN_GAP, MEDIUM_GAP, LARGE_GAP } from "../../../common/widgets/container/grid-layout.component";
const stories = storiesOf("Layouts", module);

stories.addDecorator(withKnobs);
const options = {
    None: '',
    Min: MIN_GAP,
    Medium: MEDIUM_GAP,
    Large: LARGE_GAP
  };
const defaultValue = '';


stories.add("Three columns", () => (
    <GridLayout gap={select('gap', options, defaultValue)}>
        <LeTile>
            <LeTileHeader>
                <span className="le-tile-title">Test 1234</span>
                <LeMenu classNames="personalMenu" image="fa fa-ellipsis-v" name="main">
                    <LeMenuItem
                        name="edit"
                        label="Edit"
                        image="fa fa-pencil-square-o"
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />

                    <LeMenuItem
                        name="duplicate"
                        label="Duplicate"
                        image="fa fa-files-o"
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />

                    <LeMenuItem
                        name="delete"
                        label="Delete"
                        image="fa fa-trash-o"
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />
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
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />

                    <LeMenuItem
                        name="duplicate"
                        label="Duplicate"
                        image="fa fa-files-o"
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />

                    <LeMenuItem
                        name="delete"
                        label="Delete"
                        image="fa fa-trash-o"
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />
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
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />

                    <LeMenuItem
                        name="duplicate"
                        label="Duplicate"
                        image="fa fa-files-o"
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />

                    <LeMenuItem
                        name="delete"
                        label="Delete"
                        image="fa fa-trash-o"
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />
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
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />

                    <LeMenuItem
                        name="duplicate"
                        label="Duplicate"
                        image="fa fa-files-o"
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />

                    <LeMenuItem
                        name="delete"
                        label="Delete"
                        image="fa fa-trash-o"
                        callback={name => {
                            console.log("NAME ", name);
                        }}
                    />
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
    </GridLayout>
));
