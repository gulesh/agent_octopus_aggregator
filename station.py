from __future__ import annotations

import asyncio
import logging
import os
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from dotenv import load_dotenv 

load_dotenv()

from academy.agent import action
from academy.agent import Agent
from academy.exchange.cloud.client import HttpExchangeFactory
from academy.exchange import LocalExchangeFactory
from academy.handle import Handle
from academy.logging.recommended import recommended_logging
from academy.manager import Manager
from diaspora_event_sdk import Client, get_globus_app
from diaspora_context import get_diaspora_events
from diaspora_logger import DiasporaLogConfig
from globus_compute_sdk import Executor as GCExecutor

logger = logging.getLogger("academy.station")


class ChatBot(Agent):
    def __init__(self, msg: str, topic_name: str, bot_num: int) -> None:
        super().__init__()
        self.message = msg
        self.bot_num = bot_num

    @action
    async def respond(self) -> str:
        logger.info('Bot %d', self.bot_num)
        return self.message

class Facilitator(Agent):
    def __init__(self, bot1: Handle[ChatBot], bot2: Handle[ChatBot], topic_name: str) -> None:
        super().__init__()
        self.bot1 = bot1
        self.bot2 = bot2
        self.topic_name = topic_name

    @action
    async def send_messages(self) -> None:
        msg1 = await self.bot1.respond()
        msg2 = await self.bot2.respond()
        logger.info('Bot1 says: %s', msg1)
        logger.info('Bot2 says: %s', msg2)

async def main() -> int:

    my_app = get_globus_app()
    c = Client(app=my_app)
    c.create_key()
    topic_name = "facilitator_" + uuid.uuid4().hex[:20]
    c.create_topic(topic_name)
    kafka_topic = f"{c.namespace}.{topic_name}"

    if 'ACADEMY_TUTORIAL_ENDPOINT' in os.environ:
        executor = GCExecutor(os.environ['ACADEMY_TUTORIAL_ENDPOINT'])
        factory = HttpExchangeFactory()
        log_cfg = recommended_logging()
    else:
        executor = ThreadPoolExecutor(max_workers=3)
        factory = LocalExchangeFactory()
        log_cfg = DiasporaLogConfig(kafka_topic, send_timeout=10)

    async with await Manager.from_exchange_factory(
        factory=factory,
        executors=executor,
        log_config=log_cfg,
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
            ts = datetime.fromtimestamp(event["created"]).strftime("%Y-%m-%d %H:%M:%S.%f")[:23] if "created" in event else ""
            state = event.get("academy.action_state", "")
            action = event.get("academy.action", "")
            agent = event.get("academy.agent_id") or event.get("academy.src", "")
            msg = event.get("message", "")
            agent_str = f" [{agent}]" if agent else ""
            print(f"[{ts}]{agent_str} {state or msg}" + (f" ({action})" if action else ""))

        delete_topic_result = c.delete_topic(topic_name)
        print(json.dumps(delete_topic_result, indent=2, default=str))

    return 0

if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
