import React from "common/react-vendor";
import { storiesOf } from "@storybook/react";
import {
    withKnobs,
    text,
    boolean,
    select,
    number
} from "@storybook/addon-knobs";
import "common/assets/css/font-awesome.min.css";
import LeCarousel from "common/widgets/carousel/le-carousel.component"
import LeTile from 'common/widgets/container/tile/le-tile';
import LeTileHeader from 'common/widgets/container/tile/le-tile-header';
import LeTileBody from 'common/widgets/container/tile/le-tile-body';
import LeTileFooter from 'common/widgets/container/tile/le-tile-footer';
import LeMenu from 'common/widgets/menu/le-menu';
import LeMenuItem from 'common/widgets/menu/le-menu-item';


import LeCard from 'common/widgets/container/card/le-card';
import LeCardImg from 'common/widgets/container/card/le-card-img';
import LeCardBody from 'common/widgets/container/card/le-card-body';

const stories = storiesOf("Carousel", module);

const elements = [
    { id: 1, name: 'Test1' },
    { id: 2, name: 'Test2' },
    { id: 3, name: 'Test3' },
    { id: 4, name: 'Test4' },
    { id: 5, name: 'Test5' },
    { id: 6, name: 'Test6' },
    { id: 7, name: 'Test7' },
    { id: 7, name: 'Test8' },
    { id: 7, name: 'Test9' },
    { id: 7, name: 'Test10' },
    { id: 7, name: 'Test11' },
    { id: 7, name: 'Test12' },
    { id: 7, name: 'Test113' },
    { id: 7, name: 'Test14' }
];

const utility = {
    getTileContent: () => {
        let uis = [];
        elements.forEach(element => {
            uis.push(
                <LeTile>
                    <LeTileHeader>
                        <span className="le-tile-title">{element.name}</span>
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
            );
        });
        return uis;
    },
    getCardContent: () => {
        let uis = [];
        elements.forEach(element => {
            uis.push(
                <LeCard>
                    <LeCardImg src={"/assets/images/logo_lattice.png"} />
                    <LeCardBody>
                        <p>{element.name}</p>
                    </LeCardBody>
                </LeCard>
            );
        });
        return uis;
    }
}
stories.addDecorator(withKnobs);

const style = {
    height: '99vh'
}



stories.add("Carousel - Tile", () => (
    <div style={style}>
        <LeCarousel elementsStyle={{ border: '0px solid blue', minWidth: '200px', maxWidth: '365px'}}>
            {utility.getTileContent()}
        </LeCarousel>
    </div>
));

stories.add("Carousel - Card", () => (
    <div style={style}>
        <LeCarousel numPerViewport={6} elementsStyle={{ border: '0px solid blue', minWidth: '100px', maxWidth: '200px'}}>
            {utility.getCardContent()}
        </LeCarousel>
    </div>
));