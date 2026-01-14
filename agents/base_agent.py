"""
Base Agent Class - Foundation for all autonomous agents
"""
import asyncio
import json
import redis.asyncio as redis
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from loguru import logger
import uuid


class BaseAgent:
    """Base class for all autonomous agents with Redis-based communication"""
    
    def __init__(
        self,
        agent_id: str,
        role: str,
        redis_url: str = "redis://localhost:6379",
        capabilities: list = None
    ):
        self.agent_id = agent_id
        self.role = role
        self.redis_url = redis_url
        self.capabilities = capabilities or []
        self.state = {}
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        self.running = False
        self.message_handlers: Dict[str, Callable] = {}
        
        logger.info(f"Agent {self.agent_id} ({self.role}) initialized")
    
    async def start(self):
        """Start the agent and begin listening for messages"""
        self.redis_client = await redis.from_url(self.redis_url, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        
        # Subscribe to agent's channel
        await self.pubsub.subscribe(f"agent:{self.agent_id}")
        
        # Load persisted state
        await self._load_state()
        
        self.running = True
        logger.info(f"Agent {self.agent_id} started and listening")
        
        # Start message listener in background
        asyncio.create_task(self._listen_for_messages())
    
    async def stop(self):
        """Stop the agent gracefully"""
        self.running = False
        await self._save_state()
        
        if self.pubsub:
            await self.pubsub.unsubscribe(f"agent:{self.agent_id}")
            await self.pubsub.close()
        
        if self.redis_client:
            await self.redis_client.close()
        
        logger.info(f"Agent {self.agent_id} stopped")
    
    async def send_message(self, target_agent_id: str, message: Dict[str, Any]):
        """Send message to another agent"""
        msg_payload = {
            "from": self.agent_id,
            "to": target_agent_id,
            "timestamp": datetime.utcnow().isoformat(),
            "message_id": str(uuid.uuid4()),
            "payload": message
        }
        
        await self.redis_client.publish(
            f"agent:{target_agent_id}",
            json.dumps(msg_payload)
        )
        
        logger.debug(f"{self.agent_id} -> {target_agent_id}: {message.get('type', 'unknown')}")
    
    async def _listen_for_messages(self):
        """Listen for incoming messages"""
        try:
            async for message in self.pubsub.listen():
                if message["type"] == "message":
                    await self._handle_message(message["data"])
        except Exception as e:
            logger.error(f"Message listener error in {self.agent_id}: {e}")
    
    async def _handle_message(self, raw_message: str):
        """Handle incoming message and route to appropriate handler"""
        try:
            msg = json.loads(raw_message)
            msg_type = msg.get("payload", {}).get("type")
            
            if msg_type in self.message_handlers:
                await self.message_handlers[msg_type](msg["payload"])
            else:
                logger.warning(f"No handler for message type: {msg_type}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    def register_handler(self, message_type: str, handler: Callable):
        """Register a message handler for specific message type"""
        self.message_handlers[message_type] = handler
        logger.debug(f"Registered handler for {message_type} in {self.agent_id}")
    
    async def update_state(self, updates: Dict[str, Any]):
        """Update agent state and persist to Redis"""
        self.state.update(updates)
        await self._save_state()
    
    async def _save_state(self):
        """Persist state to Redis"""
        if self.redis_client:
            await self.redis_client.set(
                f"state:{self.agent_id}",
                json.dumps(self.state)
            )
    
    async def _load_state(self):
        """Load persisted state from Redis"""
        if self.redis_client:
            state_data = await self.redis_client.get(f"state:{self.agent_id}")
            if state_data:
                self.state = json.loads(state_data)
                logger.info(f"Loaded state for {self.agent_id}")
    
    async def log_decision(self, decision: str, reasoning: str, metadata: Dict = None):
        """Log agent decision for observability"""
        log_entry = {
            "agent_id": self.agent_id,
            "timestamp": datetime.utcnow().isoformat(),
            "decision": decision,
            "reasoning": reasoning,
            "metadata": metadata or {}
        }
        
        # Store in Redis list for decision history
        await self.redis_client.lpush(
            f"decisions:{self.agent_id}",
            json.dumps(log_entry)
        )
        
        # Keep only last 1000 decisions
        await self.redis_client.ltrim(f"decisions:{self.agent_id}", 0, 999)
        
        logger.info(f"[DECISION] {self.agent_id}: {decision}")

