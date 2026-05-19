import { Component, OnDestroy, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { WebSocketService } from '../../services/websocket.service';

@Component({
  selector: 'app-notification-feed',
  standalone: true,
  imports: [CommonModule],
  template: `
    <h2>Live Feed</h2>
    <ul>
      <li *ngFor="let msg of messages">{{ msg }}</li>
    </ul>
  `,
})
export class NotificationFeedComponent implements OnInit, OnDestroy {
  messages: string[] = [];
  constructor(private ws: WebSocketService) {}

  ngOnInit(): void {
    // demo user_id — wire this up to auth once JWT is in place
    this.ws.connect('00000000-0000-0000-0000-000000000000').subscribe((m) => {
      this.messages.unshift(m);
      this.messages = this.messages.slice(0, 50);
    });
  }

  ngOnDestroy(): void {
    this.ws.disconnect();
  }
}
