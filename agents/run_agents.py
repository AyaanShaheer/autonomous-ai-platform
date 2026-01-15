"""
Agent System Runner - Starts all agents
"""
import asyncio
from orchestrator.orchestrator_agent import OrchestratorAgent
from ml_agent.ml_agent import MLAgent
from loguru import logger
import sys


async def main():
    """Start all agents and run system"""
    logger.info("Starting Autonomous AI Platform - Agent System")
    
    # Initialize agents
    orchestrator = OrchestratorAgent()
    ml_agent = MLAgent()
    
    # Start all agents
    await orchestrator.start()
    await ml_agent.start()
    
    # Wait for agents to initialize
    await asyncio.sleep(2)
    
    logger.info("All agents started successfully")
    logger.info("System ready to accept goals")
    
    # Simulate a user goal
    await orchestrator._handle_user_goal({
        "goal": "Train a classification model for anomaly detection",
        "constraints": {
            "max_training_time": 300,
            "min_accuracy": 0.85
        }
    })
    
    # Keep system running
    try:
        while True:
            # Get system status every 10 seconds
            status = await orchestrator.get_system_status()
            logger.info(f"System Status: {status}")
            
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await ml_agent.stop()
        await orchestrator.stop()
        logger.info("System shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
