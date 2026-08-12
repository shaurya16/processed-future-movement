import { Injectable, signal } from '@angular/core';
import { readPreference, writePreference } from './local-preference';

export type Theme = 'light' | 'dark' | 'auto';

const STORAGE_KEY = 'pfm.theme';
const ORDER: readonly Theme[] = ['auto', 'light', 'dark'];

@Injectable({ providedIn: 'root' })
export class ThemeStore {
  private readonly _theme = signal<Theme>(readPreference<Theme>(STORAGE_KEY, 'auto'));

  readonly theme = this._theme.asReadonly();

  constructor() {
    this.apply(this._theme());
  }

  cycle(): void {
    const next = ORDER[(ORDER.indexOf(this._theme()) + 1) % ORDER.length];
    this._theme.set(next);
    writePreference(STORAGE_KEY, next);
    this.apply(next);
  }

  /**
   * 'auto' removes the attribute rather than setting a value, so the
   * prefers-color-scheme media query governs again. Setting data-theme="auto"
   * would match neither CSS scope and strand the page in light mode.
   */
  private apply(theme: Theme): void {
    if (theme === 'auto') {
      document.documentElement.removeAttribute('data-theme');
      return;
    }
    document.documentElement.setAttribute('data-theme', theme);
  }
}
