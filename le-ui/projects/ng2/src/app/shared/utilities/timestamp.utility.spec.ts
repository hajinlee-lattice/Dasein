/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { TimestampUtility } from './timestamp.utility';

describe('Service: TimestampUtility', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [TimestampUtility]
    });
  });

  it('should ...', inject([TimestampUtility], (service: TimestampUtility) => {
    expect(service).toBeTruthy();
  }));
});
