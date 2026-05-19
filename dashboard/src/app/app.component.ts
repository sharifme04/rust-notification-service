import { Component } from '@angular/core';
import { NotificationFeedComponent } from './components/notification-feed/notification-feed.component';
import { NotificationListComponent } from './components/notification-list/notification-list.component';
import { ChannelConfigComponent } from './components/channel-config/channel-config.component';
import { MetricsPanelComponent } from './components/metrics-panel/metrics-panel.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    NotificationFeedComponent,
    NotificationListComponent,
    ChannelConfigComponent,
    MetricsPanelComponent,
  ],
  template: `
    <header>
      <div class="container" style="display:flex;align-items:center;justify-content:space-between;">
        <h1>🔔 Notification Service</h1>
        <nav>
          <a href="#">Dashboard</a>
          <a href="#">Docs</a>
        </nav>
      </div>
    </header>

    <main class="container" style="padding-top:24px;">
      <app-metrics-panel></app-metrics-panel>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
        <app-notification-feed></app-notification-feed>
        <app-channel-config></app-channel-config>
      </div>

      <app-notification-list></app-notification-list>
    </main>
  `,
})
export class AppComponent {}
