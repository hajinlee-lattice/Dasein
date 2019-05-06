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
        "value": 105,
        "additionalInfo": ['Ax', 105]
    },
    "b": {
        "label": "B",
        "value": 132,
        "additionalInfo": ['Bx', 132]
    },
    "c": {
        "label": "C",
        "value": 244,
        "additionalInfo": ['Cx', 244]
    },
    "d": {
        "label": "D",
        "value": 512,
        "additionalInfo": ['Dx', 512]
    },
    "e": {
        "label": "E",
        "value": 680,
        "additionalInfo": ['Ex', 680]
    }
};

const defaultColors = ['#70BF4A','#33BDB7'];

stories.add("barchart", () => (
    <LeBarchart 
        orientation={select('orientation={"string"}', {horizontal:'horizontal', vertical: 'vertical'}, 'horizontal')}
        type={select('type={"string"}', {plain:'plain', simple:'simple', lift:'lift'}, 'simple')}
        showValues={boolean('showValues', true)}
        showAdditionalInfo={boolean('showAdditionalInfo', false)}
        colors={array('colors={[...]}', defaultColors)}
        childrenPlacement={select('childrenPlacement={"string"}', {top: 'top', bottom: 'bottom'}, 'bottom')}
        limit={number('limit={number}', 0)}
        liftsPostfix={text('liftsPostfix', 'x')}
        data={object('data={{...}} (required)', defaultData)}
        callback={action('chart-click')}>

        <em>&lt;CHILDREN /&gt;</em>

    </LeBarchart>
));

const defaultLiftData = {
    "a": {
        "label": "A",
        "value": 3.2,
        "additionalInfo": ['3.2x', 5456]
    },
    "b": {
        "label": "B",
        "value": 1.8,
        "additionalInfo": ['1.8x', 7600]
    },
    "c": {
        "label": "C",
        "value": 0.8,
        "additionalInfo": ['0.8x', 12304]
    },
    "d": {
        "label": "D",
        "value": 0.5,
        "additionalInfo": ['0.5x', 16000]
    },
    "e": {
        "label": "E",
        "value": 0.4,
        "additionalInfo": ['0.4x', 4554]
    }
};

stories.add("liftchart", () => (
    <LeBarchart 
        orientation={select('orientation={"string"}', {horizontal:'horizontal', vertical: 'vertical'}, 'vertical')}
        type={'lift'}
        showValues={boolean('showValues', true)}
        showAdditionalInfo={boolean('showAdditionalInfo', true)}
        colors={array('colors={[...]}', [])}
        childrenPlacement={select('childrenPlacement={"string"}', {top: 'top', bottom: 'bottom'}, 'bottom')}
        limit={number('limit={number}', 0)}
        liftsPostfix={text('liftsPostfix', 'x')}
        labels={array('labels={[...]}', ['Rank',null,'Lift','Accounts'])}
        data={object('data={{...}} (required)', defaultLiftData)}
        callback={action('chart-click')}>

        <em>&lt;CHILDREN /&gt;</em>

    </LeBarchart>
));