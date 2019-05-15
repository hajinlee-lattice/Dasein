import React, { Component } from 'react';
import './breadcrumbs.scss';

export default class Breadcrumbs extends Component {
    constructor(props) {
        super(props);

        this.state = {
            breadcrumbs: this.props.breadcrumbs
        }
    }

    clickHandler = (e, crumb) => {
        this.props.onClick(crumb);
    }

    getCrumbs = () => {
        if (this.props.breadcrumbs && this.props.breadcrumbs.length > 0) {

            let crumbsArray = this.props.breadcrumbs;
            let crumbs = crumbsArray.map(function(crumb, index){
                            if (crumbsArray.length-1 != index) {
                                return ( 
                                    <li onClick={(e) => {this.clickHandler(e, crumb)}}>{crumb.label}</li>
                                );
                            } else {
                                return (
                                    <li>{crumb.label}</li>
                                );
                            }
                            
                        }, this);
            return crumbs;
        } else {
            return null;
        }
    }

    static getDerivedStateFromProps(nextProps, prevState) {
        if (nextProps.forceReload) {
            return { breadcrumbs: nextProps.breadcrumbs };
        } else {
            return null;
        }
    }

    render() {
        return (
            <ol className="breadcrumbs">
                {this.getCrumbs()}
            </ol>
        )
    }
}