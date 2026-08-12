import { Component, OnInit, inject } from '@angular/core';
import { ReportService } from './report.service';
import { IngestionStatusService } from './ingestion-status.service';
import { SourceFilePanel } from './source-file-panel';
import { KpiRow } from './kpi-row';
import { FilterBar } from './filter-bar';
import { ColumnPicker } from './column-picker';
import { RefreshControl } from './refresh-control';
import { ReportTable } from './report-table';
import { ThemeToggle } from '../shared/theme-toggle';

@Component({
  selector: 'app-report',
  imports: [
    SourceFilePanel,
    KpiRow,
    FilterBar,
    ColumnPicker,
    RefreshControl,
    ReportTable,
    ThemeToggle,
  ],
  templateUrl: './report.html',
})
export class Report implements OnInit {
  protected readonly reportService = inject(ReportService);
  private readonly statusService = inject(IngestionStatusService);

  ngOnInit(): void {
    this.reportService.load();
    this.reportService.startPolling();
    // Independent of the report: if provenance fails to load the report still works.
    this.statusService.load();
  }

  protected retry(): void {
    this.reportService.load();
  }
}
