"""
V4.6系统 - 多时间周期 + 参数扫描优化
"""

import json
import urllib.request
import time
from collections import defaultdict
from datetime import datetime

def get_kline(code='sh600000', scale=240, datalen=5000):
    if code.startswith('sh') or code.startswith('sz'):
        sym = code
    else:
        sym = f'sh{code}' if code.startswith(('6', '9')) else f'sz{code}'
    url = f'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={sym}&scale={scale}&ma=no&datalen={datalen}'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode('utf-8', errors='ignore'))
            if not data:
                return []
            return [{'day': d['day'], 'open': float(d['open']), 'high': float(d['high']), 
                    'low': float(d['low']), 'close': float(d['close']), 'volume': float(d['volume'])} 
                   for d in data]
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
    k = 50.0
    d = 50.0
    for r in rsv:
        k = (2 * k + r) / 3
        d = (2 * d + k) / 3
    j = 3 * k - 2 * d
    return k, d, j

def calc_williams_r(high, low, close):
    if high == low:
        return -50
    return (high - close) / (high - low) * -100

def calc_psy(closes):
    if len(closes) < 12:
        return 50
    ups = sum(1 for i in range(1, min(12, len(closes))) if closes[i] > closes[i-1])
    return ups / 12 * 100

def calc_cci(highs, lows, closes, period=14):
    if len(closes) < period:
        return 0
    typical = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(len(closes))]
    sma = sum(typical[-period:]) / period
    mean_dev = sum(abs(typical[i] - sma) for i in range(len(typical) - period, len(typical))) / period
    if mean_dev == 0:
        return 0
    return (typical[-1] - sma) / (0.015 * mean_dev)

def calc_boll_position(closes):
    if len(closes) < 20:
        return 0.5
    ma = sum(closes[-20:]) / 20
    variance = sum((c - ma) ** 2 for c in closes[-20:]) / 20
    std = variance ** 0.5
    upper = ma + 2 * std
    lower = ma - 2 * std
    if upper == lower:
        return 0.5
    return (closes[-1] - lower) / (upper - lower)

def generate_samples(klines):
    samples = []
    if len(klines) < 70:
        return samples
    
    for i in range(40, len(klines) - 2):
        try:
            closes = [klines[j]['close'] for j in range(max(0, i - 29), i + 1)]
            highs = [klines[j]['high'] for j in range(max(0, i - 29), i + 1)]
            lows = [klines[j]['low'] for j in range(max(0, i - 29), i + 1)]
            vols = [klines[j]['volume'] for j in range(max(0, i - 29), i + 1)]
            
            if len(closes) < 30:
                continue
            
            prev_close = klines[i-1]['close']
            pct = (klines[i]['close'] - prev_close) / prev_close * 100 if prev_close > 0 else 0
            
            pct5 = 0
            if i >= 5 and klines[i-5]['close'] > 0:
                pct5 = (klines[i]['close'] - klines[i-5]['close']) / klines[i-5]['close'] * 100
            
            pct10 = 0
            if i >= 10 and klines[i-10]['close'] > 0:
                pct10 = (klines[i]['close'] - klines[i-10]['close']) / klines[i-10]['close'] * 100
            
            pct20 = 0
            if i >= 20 and klines[i-20]['close'] > 0:
                pct20 = (klines[i]['close'] - klines[i-20]['close']) / klines[i-20]['close'] * 100
            
            pct60 = 0
            if i >= 60 and klines[i-60]['close'] > 0:
                pct60 = (klines[i]['close'] - klines[i-60]['close']) / klines[i-60]['close'] * 100
            
            rsi14 = calc_rsi(closes, 14)
            rsi6 = calc_rsi(closes, 6)
            rsi9 = calc_rsi(closes, 9)
            rsi26 = calc_rsi(closes, 26)
            
            k, d, j = calc_kdj(highs, lows, closes)
            
            williams_r = calc_williams_r(highs[-1], lows[-1], closes[-1])
            psy = calc_psy(closes)
            boll_pos = calc_boll_position(closes)
            cci = calc_cci(highs, lows, closes)
            
            avg_vol5 = sum(vols[-6:-1]) / 5 if len(vols) >= 6 else vols[-1]
            vr5 = vols[-1] / avg_vol5 if avg_vol5 > 0 else 1.0
            
            avg_vol20 = sum(vols[-21:-1]) / 20 if len(vols) >= 21 else vols[-1]
            vr20 = vols[-1] / avg_vol20 if avg_vol20 > 0 else 1.0
            
            ma5 = sum(closes[-5:]) / 5
            ma10 = sum(closes[-10:]) / 10
            ma20 = sum(closes[-20:]) / 20
            ma60 = sum(closes[-60:]) / 60 if len(closes) >= 60 else closes[-1]
            
            price_ma5 = closes[-1] / ma5 if ma5 > 0 else 1.0
            price_ma10 = closes[-1] / ma10 if ma10 > 0 else 1.0
            price_ma20 = closes[-1] / ma20 if ma20 > 0 else 1.0
            price_ma60 = closes[-1] / ma60 if ma60 > 0 else 1.0
            
            ma5_ma20 = ma5 / ma20 if ma20 > 0 else 1.0
            
            amplitude = (highs[-1] - lows[-1]) / closes[-1] * 100 if closes[-1] > 0 else 0
            
            high_low_pos = (closes[-1] - lows[-1]) / (highs[-1] - lows[-1]) if highs[-1] != lows[-1] else 0.5
            
            t1_pct = (klines[i + 1]['close'] - klines[i + 1]['open']) / klines[i + 1]['open'] * 100 if klines[i + 1]['open'] > 0 else 0
            
            sample = {
                'pct': pct, 'pct5': pct5, 'pct10': pct10, 'pct20': pct20, 'pct60': pct60,
                'rsi14': rsi14, 'rsi6': rsi6, 'rsi9': rsi9, 'rsi26': rsi26,
                'kdj_j': j, 'kdj_k': k, 'kdj_d': d,
                'williams_r': williams_r, 'psy': psy,
                'boll_pos': boll_pos, 'cci': cci,
                'vr5': vr5, 'vr20': vr20,
                'price_ma5': price_ma5, 'price_ma10': price_ma10, 
                'price_ma20': price_ma20, 'price_ma60': price_ma60,
                'ma5_ma20': ma5_ma20,
                'amplitude': amplitude, 'high_low_pos': high_low_pos,
                't1_pct': t1_pct, 't1_up': t1_pct > 0
            }
            samples.append(sample)
        except:
            continue
    return samples


print("=" * 90)
print("【V4.6系统】多时间周期 + 参数扫描优化")
print("=" * 90)

stocks = [
    ('sh600000', '浦发银行'),
    ('sh600036', '招商银行'),
    ('sh601899', '紫金矿业'),
    ('sz000001', '平安银行'),
    ('sh601398', '工商银行'),
    ('sh600519', '贵州茅台'),
    ('sz000858', '五粮液'),
    ('sz002594', '比亚迪'),
    ('sh600276', '恒瑞医药'),
    ('sh601888', '中国中免'),
]

print("\n获取K线数据...")
all_samples = []

for code, name in stocks:
    print(f"  {name}...", end=' ')
    klines = get_kline(code, scale=240, datalen=5000)
    if klines:
        samples = generate_samples(klines)
        print(f"生成 {len(samples)} 样本")
        all_samples.extend(samples)
    time.sleep(0.3)

print(f"\n总样本数: {len(all_samples)}")

if len(all_samples) == 0:
    print("ERROR: 没有生成样本!")
    exit(1)

# ============= 单因子分析 =============
print("\n" + "=" * 90)
print("【单因子分析】")
print("=" * 90)

def analyze_factor(samples, name, ranges):
    results = []
    for label, cond in ranges:
        bucket = [s for s in samples if cond(s)]
        if len(bucket) >= 30:
            up_ratio = len([x for x in bucket if x['t1_up']]) / len(bucket) * 100
            avg_ret = sum(x['t1_pct'] for x in bucket) / len(bucket)
            results.append((label, len(bucket), up_ratio, avg_ret))
    results.sort(key=lambda x: -x[2])
    return results

# RSI14细分
print("\n--- RSI14 细分 ---")
rsi_ranges = []
for i in range(10, 90, 5):
    rsi_ranges.append((f'{i}-{i+5}', lambda s, i=i: i <= s['rsi14'] < i+5))
rsi_ranges.append(('>85', lambda s: s['rsi14'] >= 85))
rsi_ranges.insert(0, ('<10', lambda s: s['rsi14'] < 10))
rsi_results = analyze_factor(all_samples, "RSI14", rsi_ranges)
for label, total, up_ratio, avg_ret in rsi_results[:12]:
    marker = "★" if up_ratio > 58 else "OK" if up_ratio > 52 else "XX" if up_ratio < 45 else ""
    print(f"  RSI14 {label:<10} {total:<6} {up_ratio:>7.1f}% {avg_ret:>+8.2f}% {marker}")

# KDJ_J细分
print("\n--- KDJ_J 细分 ---")
kdj_ranges = [
    ('<-20', lambda s: s['kdj_j'] < -20),
    ('-20~-10', lambda s: -20 <= s['kdj_j'] < -10),
    ('-10~-5', lambda s: -10 <= s['kdj_j'] < -5),
    ('-5~0', lambda s: -5 <= s['kdj_j'] < 0),
    ('0~10', lambda s: 0 <= s['kdj_j'] < 10),
    ('10~20', lambda s: 10 <= s['kdj_j'] < 20),
    ('20~50', lambda s: 20 <= s['kdj_j'] < 50),
    ('50~80', lambda s: 50 <= s['kdj_j'] < 80),
    ('80~100', lambda s: 80 <= s['kdj_j'] < 100),
    ('100~120', lambda s: 100 <= s['kdj_j'] < 120),
    ('>120', lambda s: s['kdj_j'] >= 120),
]
kdj_results = analyze_factor(all_samples, "KDJ_J", kdj_ranges)
for label, total, up_ratio, avg_ret in kdj_results[:10]:
    marker = "★" if up_ratio > 58 else "OK" if up_ratio > 52 else "XX" if up_ratio < 45 else ""
    print(f"  KDJ_J {label:<10} {total:<6} {up_ratio:>7.1f}% {avg_ret:>+8.2f}% {marker}")

# CCI
print("\n--- CCI 细分 ---")
cci_ranges = [
    ('<-150', lambda s: s['cci'] < -150),
    ('-150~-100', lambda s: -150 <= s['cci'] < -100),
    ('-100~-50', lambda s: -100 <= s['cci'] < -50),
    ('-50~0', lambda s: -50 <= s['cci'] < 0),
    ('0~50', lambda s: 0 <= s['cci'] < 50),
    ('50~100', lambda s: 50 <= s['cci'] < 100),
    ('100~150', lambda s: 100 <= s['cci'] < 150),
    ('>150', lambda s: s['cci'] >= 150),
]
cci_results = analyze_factor(all_samples, "CCI", cci_ranges)
for label, total, up_ratio, avg_ret in cci_results[:10]:
    marker = "★" if up_ratio > 58 else "OK" if up_ratio > 52 else "XX" if up_ratio < 45 else ""
    print(f"  CCI {label:<12} {total:<6} {up_ratio:>7.1f}% {avg_ret:>+8.2f}% {marker}")

# 威廉指标
print("\n--- 威廉指标 细分 ---")
williams_ranges = [
    ('>-10', lambda s: s['williams_r'] > -10),
    ('-10~-30', lambda s: -30 <= s['williams_r'] < -10),
    ('-30~-50', lambda s: -50 <= s['williams_r'] < -30),
    ('-50~-70', lambda s: -70 <= s['williams_r'] < -50),
    ('-70~-80', lambda s: -80 <= s['williams_r'] < -70),
    ('-80~-90', lambda s: -90 <= s['williams_r'] < -80),
    ('<-90', lambda s: s['williams_r'] <= -90),
]
williams_results = analyze_factor(all_samples, "威廉", williams_ranges)
for label, total, up_ratio, avg_ret in williams_results[:10]:
    marker = "★" if up_ratio > 58 else "OK" if up_ratio > 52 else "XX" if up_ratio < 45 else ""
    print(f"  威廉 {label:<10} {total:<6} {up_ratio:>7.1f}% {avg_ret:>+8.2f}% {marker}")

# 布林带位置
print("\n--- 布林带位置 细分 ---")
boll_ranges = [
    ('<0.05', lambda s: s['boll_pos'] < 0.05),
    ('0.05-0.1', lambda s: 0.05 <= s['boll_pos'] < 0.1),
    ('0.1-0.15', lambda s: 0.1 <= s['boll_pos'] < 0.15),
    ('0.15-0.2', lambda s: 0.15 <= s['boll_pos'] < 0.2),
    ('0.2-0.3', lambda s: 0.2 <= s['boll_pos'] < 0.3),
    ('0.3-0.4', lambda s: 0.3 <= s['boll_pos'] < 0.4),
    ('0.4-0.5', lambda s: 0.4 <= s['boll_pos'] < 0.5),
    ('0.5-0.6', lambda s: 0.5 <= s['boll_pos'] < 0.6),
    ('0.6-0.7', lambda s: 0.6 <= s['boll_pos'] < 0.7),
    ('0.7-0.8', lambda s: 0.7 <= s['boll_pos'] < 0.8),
    ('0.8-0.9', lambda s: 0.8 <= s['boll_pos'] < 0.9),
    ('>0.9', lambda s: s['boll_pos'] >= 0.9),
]
boll_results = analyze_factor(all_samples, "BOLL", boll_ranges)
for label, total, up_ratio, avg_ret in boll_results[:10]:
    marker = "★" if up_ratio > 58 else "OK" if up_ratio > 52 else "XX" if up_ratio < 45 else ""
    print(f"  BOLL {label:<10} {total:<6} {up_ratio:>7.1f}% {avg_ret:>+8.2f}% {marker}")

# 10日涨跌幅
print("\n--- 10日涨跌幅 细分 ---")
pct10_ranges = [
    ('<-20', lambda s: s['pct10'] < -20),
    ('-20~-15', lambda s: -20 <= s['pct10'] < -15),
    ('-15~-10', lambda s: -15 <= s['pct10'] < -10),
    ('-10~-5', lambda s: -10 <= s['pct10'] < -5),
    ('-5~0', lambda s: -5 <= s['pct10'] < 0),
    ('0~5', lambda s: 0 <= s['pct10'] < 5),
    ('5~10', lambda s: 5 <= s['pct10'] < 10),
    ('10~15', lambda s: 10 <= s['pct10'] < 15),
    ('15~20', lambda s: 15 <= s['pct10'] < 20),
    ('>20', lambda s: s['pct10'] >= 20),
]
pct10_results = analyze_factor(all_samples, "Pct10", pct10_ranges)
for label, total, up_ratio, avg_ret in pct10_results[:10]:
    marker = "★" if up_ratio > 58 else "OK" if up_ratio > 52 else "XX" if up_ratio < 45 else ""
    print(f"  Pct10 {label:<10} {total:<6} {up_ratio:>7.1f}% {avg_ret:>+8.2f}% {marker}")

# ============= 2因子组合 =============
print("\n" + "=" * 90)
print("【2因子组合深度扫描】")
print("=" * 90)

# RSI + 涨跌幅
print("\n--- RSI14 + 涨跌幅 ---")
combo_rsi_pct = defaultdict(lambda: {'total': 0, 'up': 0, 'ret': 0})

for s in all_samples:
    if s['rsi14'] < 20:
        rsi_key = 'RSIlt20'
    elif s['rsi14'] < 25:
        rsi_key = 'RSI20-25'
    elif s['rsi14'] < 30:
        rsi_key = 'RSI25-30'
    elif s['rsi14'] < 35:
        rsi_key = 'RSI30-35'
    elif s['rsi14'] < 40:
        rsi_key = 'RSI35-40'
    elif s['rsi14'] < 45:
        rsi_key = 'RSI40-45'
    elif s['rsi14'] < 50:
        rsi_key = 'RSI45-50'
    elif s['rsi14'] < 55:
        rsi_key = 'RSI50-55'
    elif s['rsi14'] < 60:
        rsi_key = 'RSI55-60'
    elif s['rsi14'] < 65:
        rsi_key = 'RSI60-65'
    elif s['rsi14'] < 70:
        rsi_key = 'RSI65-70'
    elif s['rsi14'] < 75:
        rsi_key = 'RSI70-75'
    else:
        rsi_key = 'RSIgt75'
    
    if s['pct'] < -12:
        pct_key = 'Pctlt-12'
    elif s['pct'] < -8:
        pct_key = 'Pct_-12~-8'
    elif s['pct'] < -4:
        pct_key = 'Pct_-8~-4'
    elif s['pct'] < 0:
        pct_key = 'Pct_-4~0'
    elif s['pct'] < 4:
        pct_key = 'Pct_0~4'
    elif s['pct'] < 8:
        pct_key = 'Pct_4~8'
    else:
        pct_key = 'Pctgt8'
    
    key = rsi_key + "|" + pct_key
    combo_rsi_pct[key]['total'] += 1
    if s['t1_up']:
        combo_rsi_pct[key]['up'] += 1
    combo_rsi_pct[key]['ret'] += s['t1_pct']

combo_list = []
for key, stats in combo_rsi_pct.items():
    if stats['total'] >= 40:
        up_ratio = stats['up'] / stats['total'] * 100
        avg_ret = stats['ret'] / stats['total']
        combo_list.append({'key': key, 'total': stats['total'], 'up_ratio': up_ratio, 'avg_ret': avg_ret, 'parts': key.split('|')})

combo_list.sort(key=lambda x: -x['up_ratio'])

print(f"\n{'RSI':<12} {'涨跌幅':<14} {'样本':<6} {'T+1上涨%':<12} {'平均收益%'}")
print("-" * 65)
for c in combo_list[:30]:
    print(f"{c['parts'][0]:<12} {c['parts'][1]:<14} {c['total']:<6} {c['up_ratio']:>9.1f}% {c['avg_ret']:>+10.2f}%")

# RSI + KDJ_J
print("\n--- RSI14 + KDJ_J ---")
combo_kdj = defaultdict(lambda: {'total': 0, 'up': 0, 'ret': 0})

for s in all_samples:
    if s['rsi14'] < 25:
        rsi_key = 'RSIlt25'
    elif s['rsi14'] < 35:
        rsi_key = 'RSI25-35'
    elif s['rsi14'] < 45:
        rsi_key = 'RSI35-45'
    elif s['rsi14'] < 55:
        rsi_key = 'RSI45-55'
    elif s['rsi14'] < 65:
        rsi_key = 'RSI55-65'
    elif s['rsi14'] < 75:
        rsi_key = 'RSI65-75'
    else:
        rsi_key = 'RSIgt75'
    
    if s['kdj_j'] < -20:
        kdj_key = 'KDJ_Jlt-20'
    elif s['kdj_j'] < -10:
        kdj_key = 'KDJ_J_-20~-10'
    elif s['kdj_j'] < 0:
        kdj_key = 'KDJ_J_-10~0'
    elif s['kdj_j'] < 20:
        kdj_key = 'KDJ_J_0~20'
    elif s['kdj_j'] < 50:
        kdj_key = 'KDJ_J_20~50'
    elif s['kdj_j'] < 100:
        kdj_key = 'KDJ_J_50~100'
    else:
        kdj_key = 'KDJ_Jgt100'
    
    key = rsi_key + "|" + kdj_key
    combo_kdj[key]['total'] += 1
    if s['t1_up']:
        combo_kdj[key]['up'] += 1
    combo_kdj[key]['ret'] += s['t1_pct']

combo_kdj_list = []
for key, stats in combo_kdj.items():
    if stats['total'] >= 50:
        up_ratio = stats['up'] / stats['total'] * 100
        avg_ret = stats['ret'] / stats['total']
        combo_kdj_list.append({'key': key, 'total': stats['total'], 'up_ratio': up_ratio, 'avg_ret': avg_ret, 'parts': key.split('|')})

combo_kdj_list.sort(key=lambda x: -x['up_ratio'])

print(f"\n{'RSI':<12} {'KDJ_J':<16} {'样本':<6} {'T+1上涨%':<12} {'平均收益%'}")
print("-" * 70)
for c in combo_kdj_list[:25]:
    print(f"{c['parts'][0]:<12} {c['parts'][1]:<16} {c['total']:<6} {c['up_ratio']:>9.1f}% {c['avg_ret']:>+10.2f}%")

# RSI + CCI
print("\n--- RSI14 + CCI ---")
combo_cci = defaultdict(lambda: {'total': 0, 'up': 0, 'ret': 0})

for s in all_samples:
    if s['rsi14'] < 25:
        rsi_key = 'RSIlt25'
    elif s['rsi14'] < 35:
        rsi_key = 'RSI25-35'
    elif s['rsi14'] < 45:
        rsi_key = 'RSI35-45'
    elif s['rsi14'] < 55:
        rsi_key = 'RSI45-55'
    elif s['rsi14'] < 65:
        rsi_key = 'RSI55-65'
    elif s['rsi14'] < 75:
        rsi_key = 'RSI65-75'
    else:
        rsi_key = 'RSIgt75'
    
    if s['cci'] < -100:
        cci_key = 'CCILt-100'
    elif s['cci'] < -50:
        cci_key = 'CCI_-100~-50'
    elif s['cci'] < 0:
        cci_key = 'CCI_-50~0'
    elif s['cci'] < 100:
        cci_key = 'CCI_0~100'
    else:
        cci_key = 'CCIgt100'
    
    key = rsi_key + "|" + cci_key
    combo_cci[key]['total'] += 1
    if s['t1_up']:
        combo_cci[key]['up'] += 1
    combo_cci[key]['ret'] += s['t1_pct']

combo_cci_list = []
for key, stats in combo_cci.items():
    if stats['total'] >= 50:
        up_ratio = stats['up'] / stats['total'] * 100
        avg_ret = stats['ret'] / stats['total']
        combo_cci_list.append({'key': key, 'total': stats['total'], 'up_ratio': up_ratio, 'avg_ret': avg_ret, 'parts': key.split('|')})

combo_cci_list.sort(key=lambda x: -x['up_ratio'])

print(f"\n{'RSI':<12} {'CCI':<16} {'样本':<6} {'T+1上涨%':<12} {'平均收益%'}")
print("-" * 70)
for c in combo_cci_list[:25]:
    print(f"{c['parts'][0]:<12} {c['parts'][1]:<16} {c['total']:<6} {c['up_ratio']:>9.1f}% {c['avg_ret']:>+10.2f}%")

# 10日涨跌幅 + RSI
print("\n--- 10日涨跌幅 + RSI14 ---")
combo_p10 = defaultdict(lambda: {'total': 0, 'up': 0, 'ret': 0})

for s in all_samples:
    if s['pct10'] < -20:
        p10_key = 'Pct10lt-20'
    elif s['pct10'] < -15:
        p10_key = 'Pct10_-20~-15'
    elif s['pct10'] < -10:
        p10_key = 'Pct10_-15~-10'
    elif s['pct10'] < -5:
        p10_key = 'Pct10_-10~-5'
    elif s['pct10'] < 0:
        p10_key = 'Pct10_-5~0'
    elif s['pct10'] < 5:
        p10_key = 'Pct10_0~5'
    elif s['pct10'] < 10:
        p10_key = 'Pct10_5~10'
    elif s['pct10'] < 15:
        p10_key = 'Pct10_10~15'
    else:
        p10_key = 'Pct10gt15'
    
    if s['rsi14'] < 25:
        rsi_key = 'RSIlt25'
    elif s['rsi14'] < 35:
        rsi_key = 'RSI25-35'
    elif s['rsi14'] < 45:
        rsi_key = 'RSI35-45'
    elif s['rsi14'] < 55:
        rsi_key = 'RSI45-55'
    elif s['rsi14'] < 65:
        rsi_key = 'RSI55-65'
    elif s['rsi14'] < 75:
        rsi_key = 'RSI65-75'
    else:
        rsi_key = 'RSIgt75'
    
    key = p10_key + "|" + rsi_key
    combo_p10[key]['total'] += 1
    if s['t1_up']:
        combo_p10[key]['up'] += 1
    combo_p10[key]['ret'] += s['t1_pct']

combo_p10_list = []
for key, stats in combo_p10.items():
    if stats['total'] >= 40:
        up_ratio = stats['up'] / stats['total'] * 100
        avg_ret = stats['ret'] / stats['total']
        combo_p10_list.append({'key': key, 'total': stats['total'], 'up_ratio': up_ratio, 'avg_ret': avg_ret, 'parts': key.split('|')})

combo_p10_list.sort(key=lambda x: -x['up_ratio'])

print(f"\n{'10日涨幅':<16} {'RSI':<10} {'样本':<6} {'T+1上涨%':<12} {'平均收益%'}")
print("-" * 70)
for c in combo_p10_list[:30]:
    print(f"{c['parts'][0]:<16} {c['parts'][1]:<10} {c['total']:<6} {c['up_ratio']:>9.1f}% {c['avg_ret']:>+10.2f}%")

# 10日涨跌幅 + KDJ_J
print("\n--- 10日涨跌幅 + KDJ_J ---")
combo_p10kdj = defaultdict(lambda: {'total': 0, 'up': 0, 'ret': 0})

for s in all_samples:
    if s['pct10'] < -20:
        p10_key = 'Pct10lt-20'
    elif s['pct10'] < -15:
        p10_key = 'Pct10_-20~-15'
    elif s['pct10'] < -10:
        p10_key = 'Pct10_-15~-10'
    elif s['pct10'] < -5:
        p10_key = 'Pct10_-10~-5'
    elif s['pct10'] < 0:
        p10_key = 'Pct10_-5~0'
    elif s['pct10'] < 5:
        p10_key = 'Pct10_0~5'
    elif s['pct10'] < 10:
        p10_key = 'Pct10_5~10'
    else:
        p10_key = 'Pct10gt10'
    
    if s['kdj_j'] < -20:
        kdj_key = 'KDJ_Jlt-20'
    elif s['kdj_j'] < 0:
        kdj_key = 'KDJ_J_-20~0'
    elif s['kdj_j'] < 50:
        kdj_key = 'KDJ_J_0~50'
    elif s['kdj_j'] < 100:
        kdj_key = 'KDJ_J_50~100'
    else:
        kdj_key = 'KDJ_Jgt100'
    
    key = p10_key + "|" + kdj_key
    combo_p10kdj[key]['total'] += 1
    if s['t1_up']:
        combo_p10kdj[key]['up'] += 1
    combo_p10kdj[key]['ret'] += s['t1_pct']

combo_p10kdj_list = []
for key, stats in combo_p10kdj.items():
    if stats['total'] >= 40:
        up_ratio = stats['up'] / stats['total'] * 100
        avg_ret = stats['ret'] / stats['total']
        combo_p10kdj_list.append({'key': key, 'total': stats['total'], 'up_ratio': up_ratio, 'avg_ret': avg_ret, 'parts': key.split('|')})

combo_p10kdj_list.sort(key=lambda x: -x['up_ratio'])

print(f"\n{'10日涨幅':<16} {'KDJ_J':<16} {'样本':<6} {'T+1上涨%':<12} {'平均收益%'}")
print("-" * 70)
for c in combo_p10kdj_list[:30]:
    print(f"{c['parts'][0]:<16} {c['parts'][1]:<16} {c['total']:<6} {c['up_ratio']:>9.1f}% {c['avg_ret']:>+10.2f}%")

# ============= 3因子组合 =============
print("\n" + "=" * 90)
print("【3因子组合】")
print("=" * 90)

# RSI + 涨跌幅 + VR
print("\n--- RSI14 + 涨跌幅 + VR5 ---")
combo_3f = defaultdict(lambda: {'total': 0, 'up': 0, 'ret': 0})

for s in all_samples:
    if s['rsi14'] < 25:
        rsi_key = 'RSIlt25'
    elif s['rsi14'] < 40:
        rsi_key = 'RSI25-40'
    elif s['rsi14'] < 55:
        rsi_key = 'RSI40-55'
    elif s['rsi14'] < 70:
        rsi_key = 'RSI55-70'
    else:
        rsi_key = 'RSIgt70'
    
    if s['pct'] < -10:
        pct_key = 'Pctlt-10'
    elif s['pct'] < -5:
        pct_key = 'Pct_-10~-5'
    elif s['pct'] < 0:
        pct_key = 'Pct_-5~0'
    elif s['pct'] < 5:
        pct_key = 'Pct_0~5'
    else:
        pct_key = 'Pctgt5'
    
    if s['vr5'] < 0.8:
        vr_key = 'VRlt0.8'
    elif s['vr5'] < 1.0:
        vr_key = 'VR_0.8-1.0'
    elif s['vr5'] < 1.3:
        vr_key = 'VR_1.0-1.3'
    elif s['vr5'] < 1.8:
        vr_key = 'VR_1.3-1.8'
    else:
        vr_key = 'VRgt1.8'
    
    key = rsi_key + "|" + pct_key + "|" + vr_key
    combo_3f[key]['total'] += 1
    if s['t1_up']:
        combo_3f[key]['up'] += 1
    combo_3f[key]['ret'] += s['t1_pct']

combo_3f_list = []
for key, stats in combo_3f.items():
    if stats['total'] >= 30:
        up_ratio = stats['up'] / stats['total'] * 100
        avg_ret = stats['ret'] / stats['total']
        combo_3f_list.append({'key': key, 'total': stats['total'], 'up_ratio': up_ratio, 'avg_ret': avg_ret, 'parts': key.split('|')})

combo_3f_list.sort(key=lambda x: -x['up_ratio'])

print(f"\n{'RSI':<8} {'涨跌幅':<10} {'VR':<10} {'样本':<6} {'T+1上涨%':<12} {'平均收益%'}")
print("-" * 65)
for c in combo_3f_list[:35]:
    print(f"{c['parts'][0]:<8} {c['parts'][1]:<10} {c['parts'][2]:<10} {c['total']:<6} {c['up_ratio']:>9.1f}% {c['avg_ret']:>+10.2f}%")

# 10日涨跌幅 + RSI + KDJ_J
print("\n--- 10日涨跌幅 + RSI14 + KDJ_J ---")
combo_3f2 = defaultdict(lambda: {'total': 0, 'up': 0, 'ret': 0})

for s in all_samples:
    if s['pct10'] < -20:
        p10_key = 'Pct10lt-20'
    elif s['pct10'] < -10:
        p10_key = 'Pct10_-20~-10'
    elif s['pct10'] < 0:
        p10_key = 'Pct10_-10~0'
    elif s['pct10'] < 10:
        p10_key = 'Pct10_0~10'
    else:
        p10_key = 'Pct10gt10'
    
    if s['rsi14'] < 25:
        rsi_key = 'RSIlt25'
    elif s['rsi14'] < 40:
        rsi_key = 'RSI25-40'
    elif s['rsi14'] < 55:
        rsi_key = 'RSI40-55'
    elif s['rsi14'] < 70:
        rsi_key = 'RSI55-70'
    else:
        rsi_key = 'RSIgt70'
    
    if s['kdj_j'] < -10:
        kdj_key = 'KDJ_Jlt-10'
    elif s['kdj_j'] < 0:
        kdj_key = 'KDJ_J_-10~0'
    elif s['kdj_j'] < 50:
        kdj_key = 'KDJ_J_0~50'
    elif s['kdj_j'] < 100:
        kdj_key = 'KDJ_J_50~100'
    else:
        kdj_key = 'KDJ_Jgt100'
    
    key = p10_key + "|" + rsi_key + "|" + kdj_key
    combo_3f2[key]['total'] += 1
    if s['t1_up']:
        combo_3f2[key]['up'] += 1
    combo_3f2[key]['ret'] += s['t1_pct']

combo_3f2_list = []
for key, stats in combo_3f2.items():
    if stats['total'] >= 30:
        up_ratio = stats['up'] / stats['total'] * 100
        avg_ret = stats['ret'] / stats['total']
        combo_3f2_list.append({'key': key, 'total': stats['total'], 'up_ratio': up_ratio, 'avg_ret': avg_ret, 'parts': key.split('|')})

combo_3f2_list.sort(key=lambda x: -x['up_ratio'])

print(f"\n{'10日涨幅':<12} {'RSI':<8} {'KDJ_J':<12} {'样本':<6} {'T+1上涨%':<12} {'平均收益%'}")
print("-" * 70)
for c in combo_3f2_list[:35]:
    print(f"{c['parts'][0]:<12} {c['parts'][1]:<8} {c['parts'][2]:<12} {c['total']:<6} {c['up_ratio']:>9.1f}% {c['avg_ret']:>+10.2f}%")

# ============= 最优组合汇总 =============
print("\n" + "=" * 90)
print("【最优组合 TOP 100】")
print("=" * 90)

all_best = []

for c in combo_list[:40]:
    all_best.append({'type': 'RSI+Pct', 'condition': c['key'], 'up_ratio': c['up_ratio'], 'avg_ret': c['avg_ret'], 'samples': c['total']})
for c in combo_kdj_list[:30]:
    all_best.append({'type': 'RSI+KDJ', 'condition': c['key'], 'up_ratio': c['up_ratio'], 'avg_ret': c['avg_ret'], 'samples': c['total']})
for c in combo_cci_list[:30]:
    all_best.append({'type': 'RSI+CCI', 'condition': c['key'], 'up_ratio': c['up_ratio'], 'avg_ret': c['avg_ret'], 'samples': c['total']})
for c in combo_p10_list[:30]:
    all_best.append({'type': 'Pct10+RSI', 'condition': c['key'], 'up_ratio': c['up_ratio'], 'avg_ret': c['avg_ret'], 'samples': c['total']})
for c in combo_p10kdj_list[:30]:
    all_best.append({'type': 'Pct10+KDJ', 'condition': c['key'], 'up_ratio': c['up_ratio'], 'avg_ret': c['avg_ret'], 'samples': c['total']})
for c in combo_3f_list[:50]:
    all_best.append({'type': 'RSI+Pct+VR', 'condition': c['key'], 'up_ratio': c['up_ratio'], 'avg_ret': c['avg_ret'], 'samples': c['total']})
for c in combo_3f2_list[:50]:
    all_best.append({'type': 'Pct10+RSI+KDJ', 'condition': c['key'], 'up_ratio': c['up_ratio'], 'avg_ret': c['avg_ret'], 'samples': c['total']})

all_best.sort(key=lambda x: -x['up_ratio'])

print(f"\n{'排名':<4} {'类型':<14} {'条件':<50} {'样本':<6} {'T+1上涨%':<12} {'平均收益%'}")
print("-" * 110)
for i, g in enumerate(all_best[:80], 1):
    print(f"{i:<4} {g['type']:<14} {g['condition']:<50} {g['samples']:<6} {g['up_ratio']:>9.1f}% {g['avg_ret']:>+10.2f}%")

# 保存
result = {
    'date': datetime.now().strftime('%Y-%m-%d'),
    'total_samples': len(all_samples),
    'best_combinations': all_best[:100]
}

with open('/mnt/d/AStockV4/predictions/v46_optimization.json', 'w') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print("\n")
print("结果已保存: /mnt/d/AStockV4/predictions/v46_optimization.json")