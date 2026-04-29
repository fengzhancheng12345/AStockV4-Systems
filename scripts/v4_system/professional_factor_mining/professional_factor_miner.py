"""Professional Factor Mining System - B System"""
import json, time, urllib.request
from datetime import datetime
import math, os

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

def calc_kdj(highs, lows, closes, period=9):
    if len(closes) < period:
        return 50, 50, 50
    rsv = []
    for i in range(period - 1, len(closes)):
        high = max(highs[i - period + 1:i + 1])
        low = min(lows[i - period + 1:i + 1])
        if high == low:
            rsv.append(50)
        else:
            rsv.append((closes[i] - low) / (high - low) * 100)
    k = d = 50.0
    for r in rsv:
        k = (2 * k + r) / 3
        d = (2 * d + k) / 3
    return k, d, 3 * k - 2 * d

def calc_cci(highs, lows, closes, period=14):
    if len(closes) < period:
        return 0
    typical = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(len(closes))]
    sma = sum(typical[-period:]) / period
    mean_dev = sum(abs(typical[i] - sma) for i in range(len(typical) - period, len(typical))) / period
    return (typical[-1] - sma) / (0.015 * mean_dev) if mean_dev != 0 else 0

def calc_boll_pos(closes, period=20):
    if len(closes) < period:
        return 0.5
    ma = sum(closes[-period:]) / period
    std = math.sqrt(sum((c - ma) ** 2 for c in closes[-period:]) / period)
    upper, lower = ma + 2 * std, ma - 2 * std
    return (closes[-1] - lower) / (upper - lower) if upper != lower else 0.5

def calc_vr(closes, volumes, period=20):
    if len(closes) < period + 1:
        return 100
    up = sum(volumes[i] for i in range(len(closes) - period, len(closes)) if closes[i] >= closes[i-1])
    down = sum(volumes[i] for i in range(len(closes) - period, len(closes)) if closes[i] < closes[i-1])
    return up / down * 100 if down != 0 else 200

def generate_factors(klines):
    if len(klines) < 70:
        return []
    samples = []
    for i in range(40, len(klines) - 5):
        try:
            closes = [klines[j]['close'] for j in range(max(0, i - 59), i + 1)]
            highs = [klines[j]['high'] for j in range(max(0, i - 59), i + 1)]
            lows = [klines[j]['low'] for j in range(max(0, i - 59), i + 1)]
            vols = [float(klines[j]['volume']) for j in range(max(0, i - 59), i + 1)]
            if len(closes) < 60:
                continue
            s = {'day': klines[i].get('day', '')}
            for n in [1, 5, 10, 20, 30, 60]:
                if i >= n and klines[i-n]['close'] > 0:
                    s[f'pct_{n}d'] = (closes[-1] - klines[i-n]['close']) / klines[i-n]['close'] * 100
                else:
                    s[f'pct_{n}d'] = 0
            for p in [6, 9, 14, 26]:
                s[f'rsi_{p}'] = calc_rsi(closes, p)
            k, d, j = calc_kdj(highs, lows, closes)
            s['kdj_k'], s['kdj_d'], s['kdj_j'] = k, d, j
            s['cci_14'] = calc_cci(highs, lows, closes, 14)
            s['cci_20'] = calc_cci(highs, lows, closes, 20)
            s['boll_20'] = calc_boll_pos(closes, 20)
            s['boll_30'] = calc_boll_pos(closes, 30)
            for p in [5, 10, 20]:
                s[f'vr_{p}'] = calc_vr(closes, vols, p) if len(vols) >= p + 1 else 100
            for p in [5, 10, 20, 30, 60]:
                if len(closes) >= p:
                    ma = sum(closes[-p:]) / p
                    s[f'price_ma{p}'] = closes[-1] / ma if ma > 0 else 1
                else:
                    s[f'price_ma{p}'] = 1
            s['amplitude'] = (highs[-1] - lows[-1]) / closes[-1] * 100 if closes[-1] > 0 else 0
            if i + 1 < len(klines):
                next_open = klines[i + 1]['open']
                if next_open > 0:
                    s['t1_pct'] = (klines[i + 1]['close'] - next_open) / next_open * 100
                    s['t1_up'] = 1 if s['t1_pct'] > 0 else 0
                else:
                    s['t1_pct'] = s['t1_up'] = 0
            else:
                s['t1_pct'] = s['t1_up'] = 0
            samples.append(s)
        except:
            continue
    return samples

def calc_ic(factor_vals, returns):
    pairs = [(f, t) for f, t in zip(factor_vals, returns) if f is not None and t is not None and not math.isnan(f) and not math.isnan(t)]
    if len(pairs) < 30:
        return 0
    f_vals = [p[0] for p in pairs]
    r_vals = [p[1] for p in pairs]
    mean_f, mean_r = sum(f_vals) / len(f_vals), sum(r_vals) / len(r_vals)
    cov = sum((f - mean_f) * (r - mean_r) for f, r in pairs) / len(pairs)
    std_f = math.sqrt(sum((f - mean_f) ** 2 for f in f_vals) / len(f_vals))
    std_r = math.sqrt(sum((r - mean_r) ** 2 for r in r_vals) / len(r_vals))
    return cov / (std_f * std_r) if std_f != 0 and std_r != 0 else 0

def calc_spread(factor_vals, returns):
    pairs = [(f, t) for f, t in zip(factor_vals, returns) if f is not None and t is not None and not math.isnan(f) and not math.isnan(t)]
    if len(pairs) < 30:
        return 0
    sorted_pairs = sorted(pairs, key=lambda x: x[0])
    n = len(sorted_pairs)
    top_n = max(1, n // 5)
    long_ret = sum(p[1] for p in sorted_pairs[-top_n:]) / top_n
    short_ret = sum(p[1] for p in sorted_pairs[:top_n]) / top_n
    return long_ret - short_ret

def run_test(max_stocks=100):
    print("=" * 70)
    print("B System - Professional Factor Mining")
    print("=" * 70)
    print("\n[Step 1] Getting stock list...")
    stocks = get_stock_list()
    print(f"Got {len(stocks)} stocks")
    if not stocks:
        return None
    print(f"\n[Step 2] Collecting data for {min(max_stocks, len(stocks))} stocks...")
    all_samples = []
    collected = 0
    for i, stock in enumerate(stocks[:max_stocks]):
        code, name = stock['code'], stock['name']
        klines = get_kline(code)
        if klines and len(klines) > 60:
            samples = generate_factors(klines)
            if samples:
                for s in samples:
                    s['stock_code'] = code
                    s['stock_name'] = name
                all_samples.extend(samples)
                collected += 1
        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{min(max_stocks, len(stocks))} collected: {collected} samples: {len(all_samples)}")
        time.sleep(0.05)
    print(f"\nCollected: {collected} stocks, {len(all_samples)} samples")
    if not all_samples:
        return None
    print(f"\n[Step 3] Testing factors...")
    exclude = {'day', 'stock_code', 'stock_name', 't1_pct', 't1_up'}
    factor_names = [k for k in all_samples[0].keys() if k not in exclude]
    returns = [s['t1_pct'] for s in all_samples]
    results = []
    for fname in factor_names:
        try:
            fvals = [s.get(fname) for s in all_samples]
            ic = calc_ic(fvals, returns)
            spread = calc_spread(fvals, returns)
            results.append({'name': fname, 'ic': ic, 'ic_abs': abs(ic), 'spread': spread})
        except:
            continue
    results.sort(key=lambda x: -x['ic_abs'])
    print(f"Tested {len(results)} factors")
    os.makedirs('/mnt/d/AStockV4/predictions/factor_library', exist_ok=True)
    with open('/mnt/d/AStockV4/predictions/factor_library/factor_test_results.json', 'w') as f:
        json.dump(results[:100], f, ensure_ascii=False, indent=2)
    print("\n" + "=" * 70)
    print("TOP 20 FACTORS")
    print("=" * 70)
    for i, r in enumerate(results[:20], 1):
        print(f"{i:>2}. {r['name']:<20} IC={r['ic']:>8.4f}  Spread={r['spread']:>8.4f}")
    print("=" * 70)
    return {'factors': len(results), 'samples': len(all_samples)}

if __name__ == '__main__':
    result = run_test(max_stocks=100)
    if result:
        print(f"\nDone: {result['factors']} factors, {result['samples']} samples")