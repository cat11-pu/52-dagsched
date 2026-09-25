"""dagsched.py：DAG 调度（依赖就绪、重试级联、环检测、快照恢复）。"""
from __future__ import annotations

import json


class Dag:
    PERSIST_PATH = "dagsched_state.json"

    def __init__(self, max_retries: int = 1):
        self.max_retries = max_retries
        self.tasks = {}        # task -> [deps]
        self.state = {}        # task -> pending/succeeded/failed/cancelled
        self.retries = {}      # task -> 已失败次数
        self.cancelled = set()  # 被级联取消的任务集合
        self._succ = {}        # task -> {直接后继}
        self._indegree = {}    # task -> 尚未成功的前置数（仅统计已注册前置）
        self._ready = set()    # 入度为 0 的 pending 任务

    def add(self, task: str, deps=None) -> dict:
        """注册任务；重复注册视为覆盖（状态、重试、取消全部重置）。"""
        deps = list(dict.fromkeys(deps or []))
        existed = task in self.tasks
        was_succeeded = self.state.get(task) == "succeeded"
        if existed:
            for dep in self.tasks[task]:
                self._succ.get(dep, set()).discard(task)
        self.tasks[task] = deps
        self.state[task] = "pending"
        self.retries[task] = 0
        self.cancelled.discard(task)
        successors = self._succ.setdefault(task, set())
        indegree = 0
        for dep in deps:
            self._succ.setdefault(dep, set()).add(task)
            if dep in self.state and self.state[dep] != "succeeded":
                indegree += 1
        self._indegree[task] = indegree
        self._ready.discard(task)
        if indegree == 0:
            self._ready.add(task)
        if not existed or was_succeeded:
            # 本任务现在未成功，后继要多等一个前置。
            for succ in successors:
                self._indegree[succ] += 1
                self._ready.discard(succ)
        return {"tasks": len(self.tasks)}

    def ready(self) -> list:
        """当前就绪批：前置全部成功的 pending 任务，按名字排序。"""
        return sorted(self._ready)

    def run(self, task: str, ok: bool = True) -> dict:
        if self.state.get(task) != "pending":
            return {"ran": None}
        if ok:
            self.state[task] = "succeeded"
            self._ready.discard(task)
            for succ in self._succ.get(task, ()):
                self._indegree[succ] -= 1
                if self._indegree[succ] == 0 and self.state[succ] == "pending":
                    self._ready.add(succ)
        else:
            self.retries[task] += 1
            if self.retries[task] > self.max_retries:
                self.state[task] = "failed"
                self._ready.discard(task)
                self.cancel_downstream(task)
        return {"ran": task, "state": self.state[task]}

    def cancel_downstream(self, task: str) -> dict:
        """把 task 的全部下游 pending 任务级联标成 cancelled。"""
        seen = {task}
        stack = [task]
        cancelled = []
        while stack:
            node = stack.pop()
            for succ in self._succ.get(node, ()):
                if succ in seen:
                    continue
                seen.add(succ)
                if self.state.get(succ) != "pending":
                    continue
                self.state[succ] = "cancelled"
                self.cancelled.add(succ)
                self._ready.discard(succ)
                cancelled.append(succ)
                stack.append(succ)
        return {"cancelled": sorted(cancelled)}

    def cycle(self) -> list:
        """检测依赖环，返回按名字排序后的最小环；无环返回 []。"""
        index_of = {}
        low = {}
        on_stack = set()
        stack = []
        counter = 0
        sccs = []
        for root in self.tasks:
            if root in index_of:
                continue
            index_of[root] = low[root] = counter
            counter += 1
            stack.append(root)
            on_stack.add(root)
            work = [(root, iter(self.tasks[root]))]
            while work:
                node, it = work[-1]
                descended = False
                for nxt in it:
                    if nxt not in self.tasks:
                        continue
                    if nxt not in index_of:
                        index_of[nxt] = low[nxt] = counter
                        counter += 1
                        stack.append(nxt)
                        on_stack.add(nxt)
                        work.append((nxt, iter(self.tasks[nxt])))
                        descended = True
                        break
                    if nxt in on_stack:
                        low[node] = min(low[node], index_of[nxt])
                if descended:
                    continue
                work.pop()
                if work:
                    parent = work[-1][0]
                    low[parent] = min(low[parent], low[node])
                if low[node] == index_of[node]:
                    scc = []
                    while True:
                        member = stack.pop()
                        on_stack.discard(member)
                        scc.append(member)
                        if member == node:
                            break
                    if len(scc) > 1 or scc[0] in self.tasks[scc[0]]:
                        sccs.append(sorted(scc))
        return min(sccs) if sccs else []

    def persist(self) -> dict:
        """快照落盘，并返回快照内容。"""
        blob = {
            "max_retries": self.max_retries,
            "tasks": {task: list(deps) for task, deps in self.tasks.items()},
            "state": dict(self.state),
            "retries": dict(self.retries),
            "cancelled": sorted(self.cancelled),
        }
        with open(self.PERSIST_PATH, "w", encoding="utf-8") as fh:
            json.dump(blob, fh, ensure_ascii=False)
        return blob

    def restore(self, blob) -> dict:
        """从快照（dict 或 JSON 字符串）恢复全部状态并重建索引。"""
        if isinstance(blob, (str, bytes)):
            blob = json.loads(blob)
        self.max_retries = blob["max_retries"]
        self.tasks = {task: list(deps) for task, deps in blob["tasks"].items()}
        self.state = {task: blob["state"].get(task, "pending") for task in self.tasks}
        self.retries = {task: blob["retries"].get(task, 0) for task in self.tasks}
        self.cancelled = set(blob.get("cancelled", []))
        self._succ = {}
        self._indegree = {}
        self._ready = set()
        for task, deps in self.tasks.items():
            self._succ.setdefault(task, set())
            indegree = 0
            for dep in deps:
                self._succ.setdefault(dep, set()).add(task)
                if dep in self.state and self.state[dep] != "succeeded":
                    indegree += 1
            self._indegree[task] = indegree
            if self.state[task] == "pending" and indegree == 0:
                self._ready.add(task)
        return {"restored": len(self.tasks)}

    def recover(self) -> dict:
        """模拟重启：先落盘，再从磁盘快照恢复，返回恢复后的统计。"""
        self.persist()
        with open(self.PERSIST_PATH, encoding="utf-8") as fh:
            self.restore(json.load(fh))
        return self.stats()

    def stats(self) -> dict:
        return {"tasks": len(self.tasks), "state": dict(self.state),
                "retries": dict(self.retries), "cancelled": sorted(self.cancelled),
                "max_retries": self.max_retries}
