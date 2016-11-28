/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { HttpClient } from './interceptor.service';

describe('Service: Interceptor', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [HttpClient]
    });
  });

  it('should ...', inject([HttpClient], (service: HttpClient) => {
    expect(service).toBeTruthy();
  }));
});
