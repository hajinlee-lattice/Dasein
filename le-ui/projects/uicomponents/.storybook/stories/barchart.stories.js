import React from "common/react-vendor";
import { storiesOf } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import {
  withKnobs,
  text,
  boolean,
  select,
  number,
  object,
  array
} from "@storybook/addon-knobs";
import "common/assets/css/font-awesome.min.css";
import LeBarchart from "common/widgets/charts/le-barchart";

const stories = storiesOf("Charts", module);

stories.addDecorator(withKnobs);

const defaultData = {
    "a": {
        "label": "A",
        "value": 105
    },
    "b": {
        "label": "B",
        "value": 132,
        "percent": 27
    },
    "c": {
        "label": "C",
        "value": 244
    },
    "d": {
        "label": "D",
        "value": 512
    },
    "e": {
        "label": "E",
        "value": 680
    }
};

const defaultColors = ['#70BF4A','#33BDB7'];

stories.add("barchart", () => (
    <LeBarchart 
        orientation={select('orientation={"string"}', {horizontal:'horizontal', vertical: 'vertical'}, 'horizontal')}
        colors={array('colors={[...]}', defaultColors)}
        childrenPlacement={select('childrenPlacement={"string"}', {top: 'top', bottom: 'bottom'}, 'bottom')}
        limit={number('limit={number}', 0)}
        data={object('data={{...}} (required)', defaultData)}
        callback={action('chart-click')}>

        <em>&lt;CHILDREN /&gt;</em>

    </LeBarchart>
));
