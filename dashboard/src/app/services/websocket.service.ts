import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class WebSocketService {
  private socket: WebSocket | null = null;
  private subject = new Subject<string>();

  connect(userId: string): Observable<string> {
    const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
    this.socket = new WebSocket(`${proto}://${window.location.host}/ws?user_id=${userId}`);
    this.socket.onmessage = (e) => this.subject.next(e.data);
    this.socket.onclose = () => this.subject.complete();
    return this.subject.asObservable();
  }

  disconnect(): void {
    this.socket?.close();
  }
}
