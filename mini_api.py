#!/usr/bin/env python3
import os
import requests
import threading
import time
import logging
import random
from flask import Flask, jsonify
from collections import deque
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='[MINI-API] %(asctime)s %(threadName)s: %(message)s',
    datefmt='%H:%M:%S'
)

app = Flask(__name__)

GAME_ID = "109983668079237"
BASE_URL = f"https://games.roblox.com/v1/games/{GAME_ID}/servers/Public"

MAIN_API_URL = "https://main-api-production-74c8.up.railway.app/add-pool"
MAIN_API_STATUS = "https://main-api-production-74c8.up.railway.app/status"

REQUEST_TIMEOUT = 15
PAGE_DELAY = 0.05
ID_TTL = 60 * 15
BATCH_MIN = 300
BATCH_MAX = 800
MAX_QUEUE_SIZE = 9000
TARGET_MAIN_API = 999999
TARGET_MIN = 5
TARGET_MAX = 7

# Single IPRoyal proxy
PROXIES = [
    {"url": "http://yCVVTe6Qjt6e3FQO:ofeQwnpZLSqa8LkW_country-ae@geo.iproyal.com:12321", "name": "AE 🇦🇪 IPRoyal"},
]

# Fetch pattern: ASC → DESC → ASC
FETCH_PATTERN = ["Asc", "Desc", "Asc"]

current_proxy_idx = 0
last_rotate_time = time.time()
proxy_lock = threading.Lock()

def get_current_proxy():
    return PROXIES[0]

priority_queue = deque(maxlen=MAX_QUEUE_SIZE)
server_queue = deque(maxlen=MAX_QUEUE_SIZE)
recycle_queue = deque(maxlen=MAX_QUEUE_SIZE)
sent_ids = {}
server_cache = set()

lock = threading.Lock()
stats = {"fetched": 0, "sent": 0, "duplicates": 0, "errors": 0, "ratelimits": 0}

def check_main_api_size():
    try:
        r = requests.get(MAIN_API_STATUS, timeout=3)
        if r.status_code == 200:
            return r.json().get("cache_jobs", 0)
    except:
        pass
    return 0

def cleanup_sent_ids():
    now = time.time()
    expired = [job for job, t in sent_ids.items() if t <= now]
    for job in expired:
        del sent_ids[job]
        server_cache.discard(job)
        recycle_queue.append(job)

def fetch_servers(sort_order):
    cursor = None
    consecutive_errors = 0
    
    while True:
        main_api_size = check_main_api_size()
        
        if main_api_size >= TARGET_MAIN_API:
            time.sleep(5)
            continue
        
        proxy = get_current_proxy()
        
        try:
            url = f"{BASE_URL}?sortOrder={sort_order}&limit=100"
            if cursor:
                url += f"&cursor={cursor}"
            
            r = requests.get(
                url,
                proxies={"http": proxy["url"], "https": proxy["url"]},
                timeout=REQUEST_TIMEOUT,
                verify=False
            )
            
            if r.status_code == 429:
                stats["ratelimits"] += 1
                retry_after = int(r.headers.get("Retry-After", 5))
                logging.info(f"⏸️ [{proxy['name']}] Rate limited, waiting {retry_after}s")
                time.sleep(retry_after + 1)
                continue
            
            if r.status_code != 200:
                consecutive_errors += 1
                if consecutive_errors > 5:
                    cursor = None
                    consecutive_errors = 0
                time.sleep(0.5)
                continue
            
            consecutive_errors = 0
            data = r.json().get("data", [])
            
            priority = []
            
            for s in data:
                if "id" not in s or "playing" not in s:
                    continue
                
                jid = s["id"]
                players = s["playing"]
                
                if not (TARGET_MIN <= players <= TARGET_MAX):
                    continue
                
                with lock:
                    if jid in server_cache:
                        stats["duplicates"] += 1
                        continue
                    server_cache.add(jid)
                
                priority.append(jid)
            
            with lock:
                cleanup_sent_ids()
                if len(server_cache) > MAX_QUEUE_SIZE * 2:
                    server_cache.clear()
                priority_queue.extend(priority)
                stats["fetched"] += len(priority)
            
            if priority:
                logging.info(f"✓ [{proxy['name']}] [{sort_order}] Fetched {len(priority)} | Queue: {len(priority_queue)}")
            
            cursor = r.json().get("nextPageCursor", None)
            if not cursor:
                cursor = None
                time.sleep(0.5)
            
        except Exception as e:
            stats["errors"] += 1
            logging.error(f"Error fetch: {e}")
            time.sleep(0.5)
        
        time.sleep(PAGE_DELAY)

def sender():
    while True:
        batch = []
        target = random.randint(BATCH_MIN, BATCH_MAX)
        
        with lock:
            cleanup_sent_ids()
            
            while priority_queue and len(batch) < target:
                jid = priority_queue.popleft()
                sent_ids[jid] = time.time() + ID_TTL
                batch.append(jid)
            
            while server_queue and len(batch) < target:
                jid = server_queue.popleft()
                sent_ids[jid] = time.time() + ID_TTL
                batch.append(jid)
            
            while recycle_queue and len(batch) < target:
                jid = recycle_queue.popleft()
                sent_ids[jid] = time.time() + ID_TTL
                batch.append(jid)
        
        if batch:
            try:
                r = requests.post(MAIN_API_URL, json={"servers": batch}, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    with lock:
                        stats["sent"] += len(batch)
                    logging.info(f"✓ SENT {len(batch)} → Queue: {len(priority_queue)}")
            except Exception as e:
                logging.error(f"Send error: {e}")
        
        time.sleep(0.04)

def start_threads():
    # Pattern: ASC → DESC → ASC (3 threads per pattern = 9 total)
    for i, sort_order in enumerate(FETCH_PATTERN):
        for j in range(3):
            threading.Thread(target=fetch_servers, args=(sort_order,), daemon=True, name=f"fetch-{sort_order}-{j}").start()
            time.sleep(0.05)
    
    for i in range(4):
        threading.Thread(target=sender, daemon=True, name=f"sender-{i}").start()
    
    logging.info(f"🚀 Mini API started")
    logging.info(f"🔗 Proxy: {PROXIES[0]['name']}")
    logging.info(f"🔧 Pattern: ASC → DESC → ASC (9 fetch threads + 4 sender)")
    logging.info(f"👥 Player filter: {TARGET_MIN}-{TARGET_MAX}")

start_threads()

@app.route("/")
def home():
    main_api_size = check_main_api_size()
    proxy = get_current_proxy()
    
    with lock:
        cleanup_sent_ids()
        elapsed = time.time() - start_time
        rate = stats['sent'] / elapsed if elapsed > 0 else 0
        return jsonify({
            "priority": len(priority_queue),
            "normal": len(server_queue),
            "recycle": len(recycle_queue),
            "sent_pending": len(sent_ids),
            "stats": stats,
            "main_api_size": main_api_size,
            "target": TARGET_MAIN_API,
            "rate": f"{rate:.1f} servers/sec",
            "current_proxy": proxy["name"],
            "fetch_pattern": "ASC → DESC → ASC"
        })

@app.route("/stats")
def detailed_stats():
    return jsonify({
        "queues": {
            "priority": len(priority_queue),
            "normal": len(server_queue),
            "recycle": len(recycle_queue),
            "sent": len(sent_ids)
        },
        "stats": stats,
        "current_proxy": PROXIES[0]["name"],
        "fetch_pattern": FETCH_PATTERN
    })

@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "queue_size": len(priority_queue) + len(server_queue),
        "uptime": int(time.time() - start_time)
    })

start_time = time.time()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8001))
    app.run("0.0.0.0", port, threaded=True)
