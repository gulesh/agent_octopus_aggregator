from __future__ import annotations

import asyncio
import logging
import os
import json
import time
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv 

load_dotenv()

from academy.agent import action
from academy.agent import Agent
from academy.agent import loop
from academy.exchange.cloud.client import HttpExchangeFactory
from academy.handle import Handle
from academy.logging.recommended import recommended_logging
from academy.manager import Manager
from globus_compute_sdk import Executor as GCExecutor
from diaspora_event_sdk import Client, get_globus_app
from diaspora_context import get_diaspora_events
from diaspora_logger import set_diaspora_logger

from academy.exchange import LocalExchangeFactory

EXCHANGE_ADDRESS = 'https://exchange.academy-agents.org'

class ChatBot(Agent):
    def __init__(self, msg: str, topic_name: str, bot_num: int) -> None:
        super().__init__()
        self.message = msg
        self.bot_num = bot_num

    @action
    async def respond(self) -> str:
        station_logger = logging.getLogger("station")
        station_logger.info('Bot %d', self.bot_num)
        return self.message

class Facilitator(Agent):
    def __init__(self, bot1: Handle[ChatBot], bot2: Handle[ChatBot], topic_name: str) -> None:
        super().__init__()
        self.bot1 = bot1
        self.bot2 = bot2
        self.topic_name = topic_name
        self._unregister = set_diaspora_logger(topic_name=self.topic_name, name="station")

    @action
    async def send_messages(self) -> None:
        station_logger = logging.getLogger("station")
        msg1 = await self.bot1.respond()
        msg2 = await self.bot2.respond()
        station_logger.info('Bot1 says: %s', msg1)
        station_logger.info('Bot2 says: %s', msg2)

    @action 
    async def unregister_logger(self) -> None:
        self._unregister()
    
async def main() -> int:

    my_app = get_globus_app()
    c = Client(app=my_app)
    c.create_key()
    topic_name = "facilitator_" + uuid.uuid4().hex[:20]
    c.create_topic(topic_name)
    kafka_topic = f"{c.namespace}.{topic_name}"

    if 'ACADEMY_TUTORIAL_ENDPOINT' in os.environ:
        executor = GCExecutor(os.environ['ACADEMY_TUTORIAL_ENDPOINT'])
    else:
        executor = ThreadPoolExecutor(max_workers=3)
        
    async with await Manager.from_exchange_factory(
        # factory=HttpExchangeFactory(EXCHANGE_ADDRESS, auth_method='globus'),
        factory=LocalExchangeFactory(),
        executors=executor,
        log_config=recommended_logging(),
    ) as manager:
       
        print(f"\nKafka topic: {kafka_topic}")

        bot1 = await manager.launch(ChatBot, args=('ping', topic_name, 1))
        bot2 = await manager.launch(ChatBot, args=('pong', topic_name, 2))
        fac = await manager.launch(Facilitator, args=(bot1, bot2, kafka_topic))

        time_before = int(time.time() * 1000)
        await fac.send_messages()
        await asyncio.sleep(2)

        result = get_diaspora_events(
            topic_name=kafka_topic,
            time_horizon=time_before,
        )
        for event in result["events"]:
            print(f"Consumed: {event.get('message')}")

        delete_topic_result = c.delete_topic(topic_name)
        print(json.dumps(delete_topic_result, indent=2, default=str))

        # await fac.unregister()

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
