from fastapi import APIRouter, WebSocket, WebSocketDisconnect


router = APIRouter()


@router.websocket("/api/v1/ws/events")
async def broker_events(websocket: WebSocket):

    event_manager = websocket.app.state.events

    await event_manager.connect(websocket)

    try:

        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:

        await event_manager.disconnect(websocket)