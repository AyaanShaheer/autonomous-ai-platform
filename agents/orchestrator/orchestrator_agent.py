"""
Orchestrator Agent - Central planner and coordinator
"""
from agents.base_agent import BaseAgent
from typing import Dict, Any, List
from loguru import logger
import asyncio


class OrchestratorAgent(BaseAgent):
    """
    Orchestrator Agent coordinates all other agents
    - Interprets high-level goals
    - Breaks tasks into subtasks
    - Assigns work to specialized agents
    - Maintains system state
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        super().__init__(
            agent_id="orchestrator",
            role="Orchestrator & Planner",
            redis_url=redis_url,
            capabilities=[
                "task_planning",
                "agent_coordination",
                "goal_interpretation",
                "workflow_management"
            ]
        )
        
        # Track active agents and their status
        self.active_agents = {}
        self.task_queue = []
        self.completed_tasks = []
        
        # Register message handlers
        self.register_handler("agent_ready", self._handle_agent_ready)
        self.register_handler("task_complete", self._handle_task_complete)
        self.register_handler("task_failed", self._handle_task_failed)
        self.register_handler("user_goal", self._handle_user_goal)
    
    async def _handle_agent_ready(self, payload: Dict[str, Any]):
        """Handle agent ready notification"""
        agent_id = payload.get("agent_id")
        capabilities = payload.get("capabilities", [])
        
        self.active_agents[agent_id] = {
            "status": "ready",
            "capabilities": capabilities,
            "tasks_assigned": 0
        }
        
        logger.info(f"Agent {agent_id} registered with capabilities: {capabilities}")
        await self.log_decision(
            decision=f"registered_agent_{agent_id}",
            reasoning=f"Agent {agent_id} is now available",
            metadata={"capabilities": capabilities}
        )
    
    async def _handle_user_goal(self, payload: Dict[str, Any]):
        """Handle high-level user goal and create execution plan"""
        goal = payload.get("goal")
        constraints = payload.get("constraints", {})
        
        logger.info(f"Received user goal: {goal}")
        
        # Decompose goal into tasks
        tasks = await self._decompose_goal(goal, constraints)
        
        # Add to task queue
        self.task_queue.extend(tasks)
        
        # Execute tasks
        await self._execute_task_queue()
    
    async def _decompose_goal(
        self,
        goal: str,
        constraints: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Decompose high-level goal into actionable tasks
        This is where you'd integrate LLM-based planning
        """
        # Example decomposition logic (simplified)
        tasks = []
        
        if "train" in goal.lower() and "model" in goal.lower():
            tasks = [
                {
                    "task_id": "task_001",
                    "type": "data_preparation",
                    "assigned_to": "ml_agent",
                    "description": "Prepare training data",
                    "priority": 1,
                    "status": "pending"
                },
                {
                    "task_id": "task_002",
                    "type": "model_training",
                    "assigned_to": "ml_agent",
                    "description": "Train ML model",
                    "priority": 2,
                    "status": "pending",
                    "depends_on": ["task_001"]
                },
                {
                    "task_id": "task_003",
                    "type": "model_evaluation",
                    "assigned_to": "ml_agent",
                    "description": "Evaluate model performance",
                    "priority": 3,
                    "status": "pending",
                    "depends_on": ["task_002"]
                }
            ]
        
        await self.log_decision(
            decision="goal_decomposition",
            reasoning=f"Decomposed goal into {len(tasks)} tasks",
            metadata={"goal": goal, "task_count": len(tasks)}
        )
        
        return tasks
    
    async def _execute_task_queue(self):
        """Execute tasks from queue respecting dependencies"""
        while self.task_queue:
            # Find tasks with no pending dependencies
            executable_tasks = [
                task for task in self.task_queue
                if self._dependencies_met(task)
            ]
            
            if not executable_tasks:
                logger.warning("No executable tasks; waiting for dependencies")
                await asyncio.sleep(2)
                continue
            
            # Assign tasks to agents
            for task in executable_tasks:
                await self._assign_task(task)
                self.task_queue.remove(task)
            
            # Wait before next iteration
            await asyncio.sleep(1)
    
    def _dependencies_met(self, task: Dict[str, Any]) -> bool:
        """Check if task dependencies are satisfied"""
        depends_on = task.get("depends_on", [])
        if not depends_on:
            return True
        
        completed_task_ids = [t["task_id"] for t in self.completed_tasks]
        return all(dep_id in completed_task_ids for dep_id in depends_on)
    
    async def _assign_task(self, task: Dict[str, Any]):
        """Assign task to appropriate agent"""
        target_agent = task.get("assigned_to")
        
        if target_agent not in self.active_agents:
            logger.error(f"Target agent {target_agent} not available")
            return
        
        # Send task to agent
        await self.send_message(target_agent, {
            "type": "task_assignment",
            "task": task
        })
        
        # Update agent status
        self.active_agents[target_agent]["status"] = "busy"
        self.active_agents[target_agent]["tasks_assigned"] += 1
        
        logger.info(f"Assigned task {task['task_id']} to {target_agent}")
        await self.log_decision(
            decision="task_assignment",
            reasoning=f"Assigned {task['type']} to {target_agent}",
            metadata=task
        )
    
    async def _handle_task_complete(self, payload: Dict[str, Any]):
        """Handle task completion from agent"""
        task_id = payload.get("task_id")
        agent_id = payload.get("agent_id")
        result = payload.get("result")
        
        # Mark task as complete
        self.completed_tasks.append({
            "task_id": task_id,
            "agent_id": agent_id,
            "result": result,
            "completed_at": payload.get("timestamp")
        })
        
        # Update agent status
        if agent_id in self.active_agents:
            self.active_agents[agent_id]["status"] = "ready"
        
        logger.info(f"Task {task_id} completed by {agent_id}")
        await self.log_decision(
            decision="task_completed",
            reasoning=f"Task {task_id} successfully completed",
            metadata={"agent_id": agent_id, "result": result}
        )
    
    async def _handle_task_failed(self, payload: Dict[str, Any]):
        """Handle task failure from agent"""
        task_id = payload.get("task_id")
        agent_id = payload.get("agent_id")
        error = payload.get("error")
        
        logger.error(f"Task {task_id} failed in {agent_id}: {error}")
        
        # Implement retry logic or escalation here
        await self.log_decision(
            decision="task_failed",
            reasoning=f"Task {task_id} failed: {error}",
            metadata={"agent_id": agent_id, "error": error}
        )
    
    async def get_system_status(self) -> Dict[str, Any]:
        """Get current system status"""
        return {
            "active_agents": len(self.active_agents),
            "agents": self.active_agents,
            "pending_tasks": len(self.task_queue),
            "completed_tasks": len(self.completed_tasks)
        }

