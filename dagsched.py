"""dagsched.py：DAG 调度（基线：按注册顺序，没有依赖）。"""
from __future__ import annotations


class Dag:
    def __init__(self, max_retries: int = 1):
        self.max_retries = max_retries
        self.tasks = {}
        self.order = []
        self.state = {}
        self.retries = {}
        self.cancelled = []

    def add(self, task: str, deps=None) -> dict:
        self.tasks[task] = list(deps or [])
        self.order.append(task)
        self.state[task] = "pending"
        self.retries[task] = 0
        return {"tasks": len(self.tasks)}

    def ready(self) -> list:
        """基线：谁在列表前面谁就绪。"""
        return [task for task in self.order if self.state[task] == "pending"][:1]

    def run(self, task: str, ok: bool = True) -> dict:
        """基线：不看依赖、不重试、不级联。"""
        if self.state.get(task) != "pending":
            return {"ran": None}
        self.state[task] = "succeeded" if ok else "failed"
        return {"ran": task, "state": self.state[task]}

    def cycle(self) -> list:
        raise NotImplementedError("环检测还没实现")

    def cancel_downstream(self, task: str) -> dict:
        raise NotImplementedError("级联取消还没实现")

    def recover(self) -> dict:
        raise NotImplementedError("重启恢复还没实现")

    def stats(self) -> dict:
        return {"tasks": len(self.tasks), "state": dict(self.state),
                "retries": dict(self.retries), "cancelled": list(self.cancelled),
                "max_retries": self.max_retries}
