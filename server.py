# /// script
# requires-python = ">=3.11"
# dependencies = ["fastapi", "uvicorn", "aiohttp", "PyYAML"]
# ///

import csv
import io
import json
import os
import sys
import asyncio
import aiohttp
import yaml

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# ── 路径：让 server.py 能找到 skill 里的逻辑 ──────────────────────────────
SKILL_DIR = os.path.expanduser("~/.claude/skills/taobao/scripts")
sys.path.insert(0, SKILL_DIR)

# ── 直接复用 skill 里的 API 调用逻辑 ──────────────────────────────────────
INVITE_CODE = os.getenv("MAISHOU_INVITE_CODE") or "6110440"

BASE_HEADERS = {
    "Accept": "application/json",
    "Referer": "https://hnbc018.kuaizhan.com/",
    "User-Agent": "Mozilla/5.0 AppleWebKit/537 Chrome/143 Safari/537",
}

SOURCE_NAMES = {
    "0": "全部",
    "1": "淘宝/天猫",
    "2": "京东",
    "3": "拼多多",
    "4": "苏宁",
    "5": "唯品会",
    "6": "考拉",
    "7": "抖音",
    "8": "快手",
    "10": "1688",
}

app = FastAPI(title="商品比价工具")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 工具函数 ───────────────────────────────────────────────────────────────

def parse_csv_rows(text: str) -> list[dict]:
    """把 skill 返回的 CSV 文本转成 list[dict]"""
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


async def fetch_search(session: aiohttp.ClientSession, keyword: str, source: str, page: int = 1) -> list[dict]:
    try:
        resp = await session.post(
            "https://appapi.maishou88.com/api/v1/homepage/searchList",
            headers={
                **BASE_HEADERS,
                "User-Agent": "MaiShouApp/3.7.7 (iPhone; iOS 26.3; Scale/3.00)",
                "openid": "564bdce0fa408fc9e1d5d42fd022ef0b",
                "version": "3.7.7.2",
            },
            data={
                "isCoupon": 0,
                "keyword": keyword,
                "openid": "564bdce0fa408fc9e1d5d42fd022ef0b",
                "order": "desc",
                "page": page,
                "pddListId": "",
                "sort": "",
                "sourceType": str(source),
                "user_id": "",
            },
        )
        data = await resp.json(encoding="utf-8-sig") or {}
        rows = data.get("data", [])
        if not rows:
            return []

        result = []
        for v in rows:
            result.append({
                "goodsId":       v.get("goodsId", ""),
                "source":        str(v.get("sourceType", source)),
                "sourceName":    SOURCE_NAMES.get(str(v.get("sourceType", source)), "其他"),
                "title":         v.get("title", ""),
                "shopName":      v.get("shopName", ""),
                "originalPrice": v.get("originalPrice", ""),
                "actualPrice":   v.get("actualPrice", ""),
                "couponPrice":   v.get("couponPrice", ""),
                "commission":    v.get("commission", ""),
                "monthSales":    v.get("monthSales", ""),
                "picUrl":        v.get("picUrl", ""),
            })
        return result
    except Exception as e:
        print(f"[fetch_search] source={source} error: {e}")
        return []


async def fetch_detail(session: aiohttp.ClientSession, goods_id: str, source: str) -> dict:
    params = {
        "goodsId": goods_id,
        "sourceType": str(source),
        "inviteCode": INVITE_CODE,
        "supplierCode": "",
        "activityId": "",
        "isShare": "1",
        "token": "",
    }
    resp = await session.post(
        "https://appapi.maishou88.com/api/v3/goods/detail",
        json={**params, "keyword": "", "usageScene": 5},
        headers=BASE_HEADERS,
    )
    data = await resp.json(encoding="utf-8-sig") or {}
    detail_data = data.get("data") or {}

    resp2 = await session.post(
        "https://msapi.maishou88.com/api/v1/share/getTargetUrl",
        json={**params, "isDirectDetail": 0},
        headers=BASE_HEADERS,
    )
    data2 = await resp2.json(encoding="utf-8-sig") or {}
    info = data2.get("data") or {}

    return {
        "title":    detail_data.get("title", ""),
        "buyUrl":   info.get("appUrl") or info.get("schemaUrl") or "",
        "copyText": info.get("kl") or "",
        "detail":   detail_data,
    }


# ── 路由 ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/search")
async def search_stream(
    keyword: str = Query(..., description="搜索关键词"),
    sources: str = Query("1,2,3", description="平台列表，逗号分隔"),
    page: int = Query(1, description="页码"),
):
    """SSE 流式返回各平台搜索结果"""
    source_list = [s.strip() for s in sources.split(",") if s.strip()]

    async def event_generator():
        async with aiohttp.ClientSession(headers=BASE_HEADERS) as session:
            tasks = {
                source: asyncio.create_task(
                    fetch_search(session, keyword, source, page)
                )
                for source in source_list
            }

            # 哪个平台先返回就先推给前端
            pending = set(tasks.values())
            task_to_source = {v: k for k, v in tasks.items()}

            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    source = task_to_source[task]
                    rows = task.result()
                    payload = json.dumps({
                        "source": source,
                        "sourceName": SOURCE_NAMES.get(source, "其他"),
                        "items": rows,
                    }, ensure_ascii=False)
                    yield f"data: {payload}\n\n"

            yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/detail")
async def get_detail(
    id: str = Query(..., description="商品 ID"),
    source: str = Query(..., description="平台编号"),
):
    async with aiohttp.ClientSession(headers=BASE_HEADERS) as session:
        result = await fetch_detail(session, id, source)
    return JSONResponse(result)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8765))
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
