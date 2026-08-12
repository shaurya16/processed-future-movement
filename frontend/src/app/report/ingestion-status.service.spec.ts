import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { IngestionStatusService } from './ingestion-status.service';
import { IngestionStatus } from './report-entry';

const SAMPLE: IngestionStatus = {
  configuredPath: 'sample-data/Input.txt',
  fileExists: true,
  fileSizeBytes: 127624,
  fileLastModified: '2026-08-12T09:14:00Z',
  lastIngestAt: '2026-08-12T14:31:52Z',
  fingerprint: 'fp-1',
  totalLines: 717,
  published: 717,
  skipped: 0,
  errorCount: 0,
};

describe('IngestionStatusService', () => {
  let service: IngestionStatusService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    service = TestBed.inject(IngestionStatusService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('starts with no status', () => {
    expect(service.status()).toBeNull();
    expect(service.available()).toBe(false);
  });

  it('loads the status', () => {
    service.load();
    httpMock.expectOne('/api/v1/ingest/status').flush(SAMPLE);

    expect(service.status()).toEqual(SAMPLE);
    expect(service.available()).toBe(true);
  });

  it('degrades to unavailable on failure without throwing', () => {
    // The report must not be affected by the status endpoint being down.
    service.load();
    httpMock.expectOne('/api/v1/ingest/status').flush(
      { error: 'nope' },
      { status: 500, statusText: 'Server Error' },
    );

    expect(service.status()).toBeNull();
    expect(service.available()).toBe(false);
  });
});
