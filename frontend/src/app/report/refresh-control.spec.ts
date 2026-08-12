import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { RefreshControl } from './refresh-control';
import { ReportService } from './report.service';

function setup(autoRefresh: boolean, stale = false) {
  const service = {
    autoRefresh: signal(autoRefresh),
    stale: signal(stale),
    lastLoadedAt: signal<Date | null>(new Date('2026-08-12T14:31:52Z')),
    errorMessage: signal<string | null>(stale ? 'network down' : null),
    setAutoRefresh: vi.fn(),
    refresh: vi.fn(),
  };
  TestBed.configureTestingModule({
    imports: [RefreshControl],
    providers: [{ provide: ReportService, useValue: service }],
  });
  return service;
}

describe('RefreshControl', () => {
  beforeEach(() => TestBed.resetTestingModule());

  it('disables the manual button while auto-refresh is on', async () => {
    setup(true);
    const fixture = TestBed.createComponent(RefreshControl);
    fixture.detectChanges();
    await fixture.whenStable();

    const button: HTMLButtonElement = fixture.nativeElement.querySelector(
      'button[data-testid="refresh"]',
    );
    expect(button.disabled).toBe(true);
  });

  it('enables the manual button when auto-refresh is off and wires it', async () => {
    const service = setup(false);
    const fixture = TestBed.createComponent(RefreshControl);
    fixture.detectChanges();
    await fixture.whenStable();

    const button: HTMLButtonElement = fixture.nativeElement.querySelector(
      'button[data-testid="refresh"]',
    );
    expect(button.disabled).toBe(false);
    button.click();
    expect(service.refresh).toHaveBeenCalled();
  });

  it('toggling the switch forwards the new state', async () => {
    const service = setup(true);
    const fixture = TestBed.createComponent(RefreshControl);
    fixture.detectChanges();
    await fixture.whenStable();

    const toggle: HTMLInputElement = fixture.nativeElement.querySelector(
      'input[data-testid="auto-refresh"]',
    );
    toggle.click();

    expect(service.setAutoRefresh).toHaveBeenCalledWith(false);
  });

  it('shows a stale badge when a refresh has failed', async () => {
    setup(true, true);
    const fixture = TestBed.createComponent(RefreshControl);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="stale-badge"]')).not.toBeNull();
    expect(fixture.nativeElement.textContent).toContain('network down');
  });

  it('shows no stale badge when healthy', async () => {
    setup(true, false);
    const fixture = TestBed.createComponent(RefreshControl);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="stale-badge"]')).toBeNull();
  });
});
