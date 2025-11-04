import json
import random
import redis
from typing import Dict, Any
import sys
import os
# 添加上级目录到sys.path以导入es模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from configs.config import *

redis_connection = redis.Redis(
    host=redis_config["host"],
    port=redis_config["port"],
    db=redis_config["db"],
    password=redis_config["password"], 
    decode_responses=True
)

def insert_dicts(batch_datas: list) -> None:
    """生成 batch 个字典，写入 Redis Hash，并把 key 推入待处理队列"""
    pipe = redis_connection.pipeline(transaction=True)
    for item in batch_datas:
        uid = item["id"]
        pipe.hset(REDIS_DATA_KEY_PREFIX + uid, mapping=item)
        # 把 key 推入待处理队列
        pipe.lpush(REDIS_PENDING_KEY, uid)
    pipe.execute()
    print(f"[insert] 已写入 {len(batch_datas)} 条字典并推入队列 {REDIS_PENDING_KEY}")

def fetch_one() -> Dict[str, Any]:
    """
    原子地：
    1. 从 pending 列表右侧弹出一个 uid；
    2. 把该 uid 推入 processing 列表（左侧即可）；
    3. 根据 uid 读取 Hash 并返回字典。
    如果pending为空，返回空字典。
    如果Hash数据不存在，从processing移除uid，重新获取。
    """
    while True:
        uid = redis_connection.brpoplpush(REDIS_MISS_DATA_KEY, REDIS_PROCESSING_KEY, timeout=2) #REDIS_MISS_DATA_KEY
        if uid is None:
            return {}
        data = redis_connection.hgetall(REDIS_DATA_KEY_PREFIX + uid)
        if data:  # Hash数据存在
            data["uid"] = uid          # 带上 uid，方便后续删除
            return data
        else:  # Hash数据不存在（已被删除），从processing移除，继续尝试下一个
            redis_connection.lrem(REDIS_PROCESSING_KEY, 1, uid)

def ack(uid: str) -> None:
    """把 uid 从 processing 列表移除，并删除对应 Hash"""
    pipe = redis_connection.pipeline(transaction=True)
    pipe.lrem(REDIS_PROCESSING_KEY, 1, uid)
    pipe.delete(REDIS_DATA_KEY_PREFIX + uid)
    pipe.execute()
    print(f"[ack] 已确认删除 uid={uid}")

def rollback_unprocessed() -> None:
    """
    回滚：把 processing 列表里所有 uid 重新放回 pending 列表，
    保证重启后不会丢数据。
    建议脚本启动时调用一次。
    """
    count = 0
    while True:
        uid = redis_connection.rpop(REDIS_PROCESSING_KEY)
        if uid is None:
            break
        redis_connection.lpush(REDIS_MISS_DATA_KEY, uid)# REDIS_MISS_DATA_KEY     REDIS_PENDING_KEY
        count += 1
    print(f"[rollback] 已把 {count} 个未处理完的 uid 回滚到 pending")

def failed_rollback(uid):
    """
    失败回滚：处理失败，把 uid 从 processing 移回 pending，稍后重试
    """
    redis_connection.lrem(REDIS_PROCESSING_KEY, 1, uid)
    redis_connection.lpush(REDIS_MISS_DATA_KEY, uid)

# 公告PDF解析有问题，此部分先放到REDIS_MISS_DATA_KEY队列中去，待正确解析后，再从REDIS_MISS_DATA_KEY队列中移回REDIS_PENDING_KEY队列中
# count = 0
# pipe = redis_connection.pipeline(transaction=True)
# with open("./utils/wind_page_miss_data.jsonl", "r") as f:
#     for line in f:
#         data = json.loads(line)
#         uid = data["_id"]
#         if redis_connection.exists(REDIS_DATA_KEY_PREFIX+uid) > 0:
#             count += 1
#             print(uid)
#             # 将UID从REDIS_PENDING_KEY移除并添加到REDIS_MISS_DATA_KEY
#             pipe.lrem(REDIS_PENDING_KEY, 0, uid)  # 从pending队列中移除该uid（移除所有匹配项）
#             pipe.lpush(REDIS_MISS_DATA_KEY, uid)  # 添加到miss_data队列

# # 执行批量操作
# if count > 0:
#     pipe.execute()
#     print(f"[miss_data] 已将 {count} 个UID从 {REDIS_PENDING_KEY} 移动到 {REDIS_MISS_DATA_KEY}")
# else:
#     print("没有找到存在的数据")
# print(f"总计处理数据: {count}")