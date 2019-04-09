import React, { Component } from "../../react-vendor";
import './grid-layout.scss';

export const MIN_GAP = 'min-gap';
export const MEDIUM_GAP = 'medium-gap';
export const LARGE_GAP = 'large-gap';

const containerStyle = {};
class GridLayout extends Component {

    constructor(props) {
        super(props);
        // if(props.gridStyle){
        //     this.gridStyle = Object.assign(this.gridStyle, props.gridStyle);
        // }
    }
    getStyle() {
        let gridStyle = {};
        if(this.props.min && this.props.min > 0){
            gridStyle.gridTemplateColumns = `${'repeat( auto-fit, minmax('}${this.props.min}${'px, 1fr) )'}`;
        }else if(this.props.numCol && this.props.numCol > 0){
            let perc = 100/this.props.numCol;
            if(this.props.gap){

            }
            gridStyle.gridTemplateColumns = `${'repeat( auto-fit, minmax('}${perc}${'%, 1fr) )'}`;
        }
        return gridStyle;
    }
    render() {
        
        return (
            <div className="le-grid-layout-main">
                <div className="le-grid-layout-container">
                    <div className={`le-grid-layout ${this.props.gap ? this.props.gap : ''} ${this.props.classNames ? this.props.classNames : ''}`}>
                        {this.props.children}
                    </div>
                </div>
            </div>
        );
    }
}

export default GridLayout;
