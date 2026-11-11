import asyncio

from fastapi import WebSocket


class EventManager:

    def __init__(self):

        self.connections = set()
        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):

        await websocket.accept()

        async with self.lock:
            self.connections.add(websocket)

    async def disconnect(self, websocket: WebSocket):

        async with self.lock:
            self.connections.discard(websocket)

    async def broadcast(self, event: dict):

        async with self.lock:
            connections = list(self.connections)

        disconnected = []

        for websocket in connections:

            try:
                await websocket.send_json(event)

            except Exception:
                disconnected.append(websocket)

        if disconnected:

            async with self.lock:

                for websocket in disconnected:
                    self.connections.discard(websocket)