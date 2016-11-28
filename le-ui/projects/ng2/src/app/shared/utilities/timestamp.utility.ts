import { Injectable } from '@angular/core';

@Injectable()
export class TimestampUtility {

    constructor() { }

    public getDays(timestamp): number {
        var MILLISECOND_PER_DAY = 24 * 60 * 60 * 1000;
        var numDaysAgoPasswordLastModified = Math.floor((Date.now() - timestamp) / MILLISECOND_PER_DAY);

        return numDaysAgoPasswordLastModified;
    }
    
    public checkLastModified(timestamp): boolean {
        return this.getDays(timestamp) >= 90;
    }

}
