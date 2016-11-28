import { Component, ViewContainerRef, ViewEncapsulation, OnInit } from '@angular/core';
import { TimeoutUtility } from "./shared/utilities/timeout.utility";
import { Overlay } from 'angular2-modal';

@Component({
  selector: 'app-root',
  template: `<div id="modal_container"></div><ui-view></ui-view>`,
  styles: []
})
export class AppComponent implements OnInit {

    constructor(
        private timeoutUtility: TimeoutUtility,
        overlay: Overlay, 
        vcRef: ViewContainerRef
    ) {
        timeoutUtility.init();
        overlay.defaultViewContainer = vcRef;
    }

    ngOnInit() { }
    
}