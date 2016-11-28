/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { TimeoutUtility } from './timeout.utility';

describe('Service: Timeout', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [TimeoutUtility]
    });
  });

  it('should ...', inject([TimeoutUtility], (service: TimeoutUtility) => {
    expect(service).toBeTruthy();
  }));
});
