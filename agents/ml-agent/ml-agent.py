"""
ML Agent - Handles model training, evaluation, and selection
"""
from agents.base_agent import BaseAgent
from typing import Dict, Any
from datetime import datetime
from loguru import logger
import asyncio


class MLAgent(BaseAgent):
    """
    ML Agent manages the ML lifecycle
    - Selects ML algorithms
    - Trains models
    - Evaluates performance
    - Registers models
    - Decides when retraining is needed
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        super().__init__(
            agent_id="ml_agent",
            role="ML Engineer",
            redis_url=redis_url,
            capabilities=[
                "model_training",
                "model_evaluation",
                "algorithm_selection",
                "data_preparation"
            ]
        )
        
        self.models = {}
        self.training_history = []
        
        # Register message handlers
        self.register_handler("task_assignment", self._handle_task_assignment)
        self.register_handler("retrain_request", self._handle_retrain_request)
    
    async def start(self):
        """Start ML Agent and notify orchestrator"""
        await super().start()
        
        # Notify orchestrator that agent is ready
        await self.send_message("orchestrator", {
            "type": "agent_ready",
            "agent_id": self.agent_id,
            "capabilities": self.capabilities
        })
    
    async def _handle_task_assignment(self, payload: Dict[str, Any]):
        """Handle task assignment from orchestrator"""
        task = payload.get("task")
        task_type = task.get("type")
        
        logger.info(f"Received task: {task.get('task_id')} - {task_type}")
        
        # Route to appropriate handler
        if task_type == "data_preparation":
            result = await self._prepare_data(task)
        elif task_type == "model_training":
            result = await self._train_model(task)
        elif task_type == "model_evaluation":
            result = await self._evaluate_model(task)
        else:
            result = {"status": "error", "message": f"Unknown task type: {task_type}"}
        
        # Notify orchestrator of completion
        await self._notify_task_complete(task, result)
    
    async def _prepare_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare training data"""
        logger.info("Preparing training data...")
        
        await self.log_decision(
            decision="data_preparation_started",
            reasoning="Preparing data for model training",
            metadata=task
        )
        
        # Simulate data preparation
        await asyncio.sleep(2)
        
        result = {
            "status": "success",
            "dataset_size": 10000,
            "features": 20,
            "data_path": "/data/training_dataset.csv"
        }
        
        await self.log_decision(
            decision="data_preparation_complete",
            reasoning="Data preparation successful",
            metadata=result
        )
        
        return result
    
    async def _train_model(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Train ML model"""
        logger.info("Training ML model...")
        
        await self.log_decision(
            decision="model_training_started",
            reasoning="Beginning model training process",
            metadata=task
        )
        
        # Simulate model training
        await asyncio.sleep(5)
        
        # Mock model metrics
        model_id = f"model_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        result = {
            "status": "success",
            "model_id": model_id,
            "algorithm": "RandomForest",
            "training_time_seconds": 5,
            "accuracy": 0.87,
            "precision": 0.85,
            "recall": 0.89
        }
        
        # Store model info
        self.models[model_id] = result
        self.training_history.append(result)
        
        await self.log_decision(
            decision="model_training_complete",
            reasoning=f"Model trained successfully with accuracy: {result['accuracy']}",
            metadata=result
        )
        
        return result
    
    async def _evaluate_model(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluate model performance"""
        logger.info("Evaluating model performance...")
        
        await self.log_decision(
            decision="model_evaluation_started",
            reasoning="Evaluating trained model",
            metadata=task
        )
        
        # Simulate evaluation
        await asyncio.sleep(2)
        
        result = {
            "status": "success",
            "test_accuracy": 0.85,
            "f1_score": 0.86,
            "recommendation": "deploy_to_production"
        }
        
        await self.log_decision(
            decision="model_evaluation_complete",
            reasoning=f"Model evaluation complete. Recommendation: {result['recommendation']}",
            metadata=result
        )
        
        return result
    
    async def _handle_retrain_request(self, payload: Dict[str, Any]):
        """Handle model retraining request (triggered by drift detection)"""
        reason = payload.get("reason", "scheduled_retrain")
        
        logger.info(f"Retrain requested: {reason}")
        
        # Create retrain task and notify orchestrator
        await self.send_message("orchestrator", {
            "type": "user_goal",
            "goal": "retrain_model",
            "constraints": {"reason": reason}
        })
    
    async def _notify_task_complete(self, task: Dict[str, Any], result: Dict[str, Any]):
        """Notify orchestrator that task is complete"""
        await self.send_message("orchestrator", {
            "type": "task_complete",
            "task_id": task.get("task_id"),
            "agent_id": self.agent_id,
            "result": result,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def get_model_inventory(self) -> Dict[str, Any]:
        """Get current model inventory"""
        return {
            "total_models": len(self.models),
            "models": self.models,
            "training_runs": len(self.training_history)
        }
