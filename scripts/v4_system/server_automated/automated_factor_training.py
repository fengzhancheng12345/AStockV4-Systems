"""
C系统 - 服务器自动化因子训练
=====================================
使用与B系统统一的经验库

经验库同步流程:
1. 从GitHub拉取最新经验库
2. 全市场数据采集
3. 因子测试
4. 结果合并到经验库
5. 推送回GitHub

存储结构:
- 经验库: /root/astock_c/data/unified_experience_db.json
- 因子结果: /root/astock_c/data/factor_results/
- 日志: /root/astock_c/logs/
"""

import json
import time
import os
import sys
import logging
import urllib.request
from datetime import datetime
import base64
import math

# ==================== 配置 ====================
CONFIG = {
    'version': 'C2.0',
    'data_dir': '/root/astock_c/data',
    'log_dir': '/root/astock_c/logs',
    'github_repo': 'fengzhancheng12345/AStockV4-Systems',
    'github_token': os.environ.get('GITHUB_TOKEN', ''),
    'max_stocks': 500,
    'ic_threshold': 0.01,
}

os.makedirs(CONFIG['data_dir'], exist_ok=True)
os.makedirs(CONFIG['log_dir'], exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f"{CONFIG['log_dir']}/c_system_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('astock_c')

# ==================== 经验库管理 ====================
EXPERIENCE_FILE = f"{CONFIG['data_dir']}/unified_experience_db.json"

def load_experience():
    if os.path.exists(EXPERIENCE_FILE):
        with open(EXPERIENCE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'factors': {}, 'rules': {}, 'metadata': {'version': 'C2.0'}}

def save_experience(db):
    db['metadata']['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    with open(EXPERIENCE_FILE, 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def sync_from_github():
    """从GitHub拉取经验库"""
    if not CONFIG['github_token']:
        logger.warning("No GitHub token, skipping sync")
        return
    try:
        import urllib.request
        url = f"https://api.github.com/repos/{CONFIG['github_repo']}/contents/predictions/experience/unified_experience_db.json"
        req = urllib.request.Request(url, headers={
            'Authorization': f'token {CONFIG["github_token"]}',
            'Accept': 'application/vnd.github.v3+json'
        })
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode())
            content = base64.b64decode(data['content']).decode('utf-8')
            with open(EXPERIENCE_FILE, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info(f"Synced experience from GitHub: {len(content)} bytes")
    except Exception as e:
        logger.warning(f"GitHub sync failed: {e}")

def sync_to_github():
    """推送经验库到GitHub"""
    if not CONFIG['github_token']:
        return
    try:
        content = open(EXPERIENCE_FILE, 'r', encoding='utf-8').read()
        url = f"https://api.github.com/repos/{CONFIG['github_repo']}/contents/predictions/experience/unified_experience_db.json"
        # Get SHA
        req = urllib.request.Request(url, headers={
            'Authorization': f'token {CONFIG["github_token"]}',
            'Accept': 'application/vnd.github.v3+json'
        })
        sha = ''
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                sha = json.loads(r.read().decode()).get('sha', '')
        except:
            pass
        data = {
            'message': f'C系统更新经验库 {datetime.now().strftime("%Y-%m-%d %H:%M")}',
            'content': base64.b64encode(content.encode()).decode(),
        }
        if sha:
            data['sha'] = sha
        req = urllib.request.Request(url, data=json.dumps(data).encode(), headers={
            'Authorization': f'token {CONFIG["github_token"]}',
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.github.v3+json'
        }, method='PUT')
        with urllib.request.urlopen(req, timeout=30) as r:
            logger.info("Pushed experience to GitHub")
    except Exception as e:
        logger.warning(f"GitHub push failed: {e}")

# ==================== 数据采集 ====================
def get_stock_list():
    stocks = []
    for page in range(1, 50):
        url = f'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=100&sort=symbol&asc=1&node=hs_a&symbol=&_s_r_a=page'
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn'})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = r.read().decode('gbk', errors='ignore')
                items = json.loads(data) if data.startswith('[') else []
                if not items:
                    break
                for item in items:
                    code = item.get('symbol', '')
                    name = item.get('name', '')
                    if code and name and not name.startswith('*') and '退' not in name:
                        if code.startswith(('sh6', 'sz0', 'sz3')):
                            stocks.append({'code': code, 'name': name})
                if len(items) < 100:
                    break
        except:
            break
    return stocks

def get_kline(code):
    if not code.startswith(('sh', 'sz')):
        code = f'sh{code}' if code.startswith(('6', '9')) else f'sz{code}'
    url = f'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={code}&scale=240&datalen=500&ma=no'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn'})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode('utf-8', errors='ignore'))
            return [{'day': d['day'], 'open': float(d['open']), 'high': float(d['high']),
                'low': float(d['low']), 'close': float(d['close']), 'volume': float(d['volume'])} for d in data]
    except:
        return []

# ==================== 因子计算 ====================
def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))

def calc_vr(closes, volumes, period=20):
    if len(closes) < period + 1:
        return 100
    up = sum(volumes[i] for i in range(len(closes) - period, len(closes)) if closes[i] >= closes[i-1])
    down = sum(volumes[i] for i in range(len(closes) - period, len(closes)) if closes[i] < closes[i-1])
    return up / down * 100 if down != 0 else 200

def generate_sample(klines, i):
    """为第i天生成一个样本"""
    try:
        closes = [klines[j]['close'] for j in range(max(0, i - 59), i + 1)]
        vols = [float(klines[j]['volume']) for j in range(max(0, i - 59), i + 1)]
        if len(closes) < 60:
            return None
        s = {}
        for n in [1, 5, 10, 20]:
            if i >= n and klines[i-n]['close'] > 0:
                s[f'pct_{n}d'] = (closes[-1] - klines[i-n]['close']) / klines[i-n]['close'] * 100
            else:
                s[f'pct_{n}d'] = 0
        for p in [6, 14]:
            s[f'rsi_{p}'] = calc_rsi(closes, p)
        s['vr_20'] = calc_vr(closes, vols, 20)
        if i + 1 < len(klines) and klines[i + 1]['open'] > 0:
            s['t1_pct'] = (klines[i + 1]['close'] - klines[i + 1]['open']) / klines[i + 1]['open'] * 100
        else:
            s['t1_pct'] = 0
        return s
    except:
        return None

def calc_ic(factor_vals, returns):
    pairs = [(f, t) for f, t in zip(factor_vals, returns) if f is not None and t is not None]
    if len(pairs) < 30:
        return 0
    f_vals = [p[0] for p in pairs]
    r_vals = [p[1] for p in pairs]
    mean_f, mean_r = sum(f_vals) / len(f_vals), sum(r_vals) / len(r_vals)
    cov = sum((f - mean_f) * (r - mean_r) for f, r in pairs) / len(pairs)
    std_f = math.sqrt(sum((f - mean_f) ** 2 for f in f_vals) / len(f_vals))
    std_r = math.sqrt(sum((r - mean_r) ** 2 for r in r_vals) / len(r_vals))
    return cov / (std_f * std_r) if std_f != 0 and std_r != 0 else 0

# ==================== 主流程 ====================
def run():
    logger.info("=" * 60)
    logger.info("C系统启动 - 自动化因子训练")
    logger.info("=" * 60)
    
    # 1. 同步经验库
    logger.info("[1/5] 同步GitHub经验库...")
    sync_from_github()
    exp_db = load_experience()
    logger.info(f"  当前经验库: {len(exp_db.get('factors', {}))} 因子, {len(exp_db.get('rules', {}))} 规则")
    
    # 2. 获取股票列表
    logger.info("[2/5] 获取股票列表...")
    stocks = get_stock_list()
    logger.info(f"  获取到 {len(stocks)} 股票")
    if not stocks:
        logger.error("无法获取股票列表")
        return
    
    # 3. 采集数据并测试
    logger.info("[3/5] 采集数据并测试因子...")
    all_samples = []
    for i, stock in enumerate(stocks[:CONFIG['max_stocks']]):
        klines = get_kline(stock['code'])
        if klines and len(klines) > 70:
            for j in range(40, len(klines) - 5):
                s = generate_sample(klines, j)
                if s:
                    s['stock_code'] = stock['code']
                    all_samples.append(s)
        if (i + 1) % 50 == 0:
            logger.info(f"  进度: {i+1}/{min(CONFIG['max_stocks'], len(stocks))} 样本: {len(all_samples)}")
        time.sleep(0.03)
    logger.info(f"  共采集 {len(all_samples)} 样本")
    
    if not all_samples:
        logger.warning("没有采集到样本")
        return
    
    # 4. 测试因子
    logger.info("[4/5] 测试因子...")
    exclude = {'t1_pct', 'stock_code'}
    factor_names = [k for k in all_samples[0].keys() if k not in exclude]
    returns = [s['t1_pct'] for s in all_samples]
    new_factors = []
    for fname in factor_names:
        fvals = [s.get(fname) for s in all_samples]
        ic = calc_ic(fvals, returns)
        if abs(ic) >= CONFIG['ic_threshold']:
            new_factors.append({'name': fname, 'ic': ic, 'ic_abs': abs(ic)})
    new_factors.sort(key=lambda x: -x['ic_abs'])
    logger.info(f"  发现 {len(new_factors)} 个有效因子(|IC| >= {CONFIG['ic_threshold']})")
    
    # 5. 合并到经验库
    logger.info("[5/5] 合并到经验库...")
    for f in new_factors:
        exp_db.setdefault('factors', {})[f['name']] = {
            'ic': round(f['ic'], 6),
            'ic_abs': round(abs(f['ic']), 6),
            'samples': len(all_samples),
            'updated': datetime.now().strftime('%Y-%m-%d')
        }
    save_experience(exp_db)
    logger.info(f"  经验库更新: {len(exp_db.get('factors', {}))} 因子")
    
    # 6. 推送到GitHub
    logger.info("[6/6] 推送到GitHub...")
    sync_to_github()
    
    # 打印结果
    logger.info("=" * 60)
    logger.info("TOP 10 因子")
    logger.info("=" * 60)
    for i, f in enumerate(new_factors[:10], 1):
        logger.info(f"  {i:>2}. {f['name']:<15} IC={f['ic']:>8.4f}")
    logger.info("=" * 60)
    logger.info("完成!")

if __name__ == '__main__':
    run()