import json
from channels.generic.websocket import AsyncWebsocketConsumer

class NotificationConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        self.user_id = self.scope["url_route"]["kwargs"]["user_id"]
        self.group_name = f"notifications_{self.user_id}"

        await self.channel_layer.group_add(
            self.group_name,
            self.channel_name
        )
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.group_name,
            self.channel_name
        )

    async def notification_update(self, event):
        await self.send(text_data=json.dumps({
            "payload_type": "notification.update",
            "id": event["id"],
            "task_id": event["task_id"],
            "filename": event["filename"],
            "status": event["status"],
            "progress": event.get("progress", 0),
            "messages": event["messages"],
        }))


class FinalNotificationConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.user_id = self.scope["url_route"]["kwargs"]["user_id"]
        self.group_name = f"final_notification_{self.user_id}"

        await self.channel_layer.group_add(self.group_name, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.group_name, self.channel_name)

    async def notification_update(self, event):
        await self.send(text_data=json.dumps({
            "payload_type": "notification.update",
            "id": event["id"],
            "task_id": event["task_id"],
            "filename": event["filename"],
            "status": event["status"],
            "progress": event.get("progress", 0),
            "messages": event["messages"],
        }))
