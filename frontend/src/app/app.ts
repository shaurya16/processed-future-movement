import { Component } from '@angular/core';
import { Report } from './report/report';

@Component({
  selector: 'app-root',
  imports: [Report],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {}
