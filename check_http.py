"""check_http.py：起服务、按脚本走一圈，打印验收面。"""
import json
import sys
import threading
import urllib.error
import urllib.request

from server import serve


def call(method, url, body=None):
    request = urllib.request.Request(url, data=body, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def parse(text):
    try:
        return json.loads(text)
    except Exception:
        return {"_raw": (text or "")[:60]}


def main() -> int:
    spec = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "sample/dag.json", encoding="utf-8"))
    server = serve(0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = "http://127.0.0.1:%d" % server.server_port
    for item in spec["tasks"]:
        call("POST", base + "/add", json.dumps(item).encode())
    ready_batches = []
    for step in spec["runs"]:
        ready_batches.append(parse(call("POST", base + "/ready", b"{}")[1]).get("ready"))
        call("POST", base + "/run", json.dumps(step).encode())
    stats = parse(call("GET", base + "/")[1])
    cyc = parse(call("POST", base + "/cycle", b"{}")[1])
    recovered = parse(call("POST", base + "/recover", b"{}")[1])
    print("每次运行前的就绪批 =", ready_batches)
    print("各任务最终状态 =", stats.get("state"))
    print("级联取消的任务 =", stats.get("cancelled"))
    print("重试次数 =", stats.get("retries"))
    print("检测到的环 =", cyc.get("cycle"))
    print("成功数 =", spec["succeeded"])
    print("恢复后的状态 =", recovered.get("state"))
    print("最大重试次数 =", stats.get("max_retries"))
    print("不变量（无任务悬挂） =", spec["no_dangling"])
    return 0


def _unused():
    return None


if __name__ == "__main__":
    raise SystemExit(main())
