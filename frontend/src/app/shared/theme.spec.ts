import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it } from 'vitest';
import { ThemeStore } from './theme';

describe('ThemeStore', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
    TestBed.resetTestingModule();
  });

  it('defaults to auto and stamps no attribute', () => {
    const store = TestBed.inject(ThemeStore);

    expect(store.theme()).toBe('auto');
    expect(document.documentElement.hasAttribute('data-theme')).toBe(false);
  });

  it('cycles light -> dark -> auto', () => {
    const store = TestBed.inject(ThemeStore);

    store.cycle();
    expect(store.theme()).toBe('light');
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');

    store.cycle();
    expect(store.theme()).toBe('dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

    store.cycle();
    expect(store.theme()).toBe('auto');
    // auto must REMOVE the attribute so the media query governs again.
    expect(document.documentElement.hasAttribute('data-theme')).toBe(false);
  });

  it('restores a persisted choice on construction', () => {
    localStorage.setItem('pfm.theme', '"dark"');

    const store = TestBed.inject(ThemeStore);

    expect(store.theme()).toBe('dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });
});
