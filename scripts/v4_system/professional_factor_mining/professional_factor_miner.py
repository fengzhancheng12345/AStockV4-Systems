"""
Professional Factor Mining System (B系统 - 本地)
专业因子挖掘系统

功能：
1. 全市场股票数据采集
2. 候选因子生成（100+因子）
3. IC分析、分层回测、多空组合测试
4. 样本外验证
5. 因子去相关性筛选
6. 机器学习因子组合优化
"""

import json
import time
import random
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from collections import defaultdict
import math
import os
import sys

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ==================== 数据采集 ====================

def get_kline_tencent(code='sh600000', scale=240, datalen=5000):
    """腾讯财经K线接口"""
    if code.startswith('sh') or code.startswith('sz'):
        sym = code
    else:
        sym = f'sh{code}' if code.startswith(('6', '9')) else f'sz{code}'
    
    url = f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_dayhfq&param={sym},day,,,,{datalen},qfq'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.qq.com'})
        with urllib.request.urlopen(req, timeout=15) as r:
            text = r.read().decode('utf-8', errors='ignore')
            text = text.replace('kline_dayhfq=', '', 1).strip()
            data = json.loads(text)
            if not data or 'data' not in data or sym not in data['data']:
                return []
            qfqdata = data['data'][sym]['qfqday']
            if not qfqdata:
                return []
            result = []
            for d in qfqdata[-datalen:]:
                if len(d) >= 6:
                    result.append({
                        'day': d[0], 'open': float(d[1]), 'high': float(d[2]),
                        'low': float(d[3]), 'close': float(d[4]), 'volume': float(d[5])
                    })
            return result
    except Exception as e:
        return []

def get_realtime_quote(code='sh600000'):
    """获取实时行情"""
    if code.startswith('sh') or code.startswith('sz'):
        sym = code
    else:
        sym = f'sh{code}' if code.startswith(('6', '9')) else f'sz{code}'
    
    url = f'https://qt.gtimg.cn/q={sym}'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            text = r.read().decode('gbk', errors='ignore')
            parts = text.split('~')
            if len(parts) > 40:
                return {
                    'code': code, 'name': parts[1], 'price': float(parts[3]),
                    'pct': float(parts[32]) if parts[32] else 0,
                    'volume': float(parts[6]) if parts[6] else 0,
                    'amount': float(parts[37]) if parts[37] else 0,
                }
    except:
        pass
    return None

def get_stock_list_a_share():
    """获取全市场A股列表"""
    url = 'https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14,f3,f6,f7,f8,f104,f105,f106'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://quote.eastmoney.com'})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode('utf-8', errors='ignore'))
            stocks = []
            if data and 'data' in data and 'diff' in data['data']:
                for item in data['data']['diff']:
                    code = item.get('f12', '')
                    name = item.get('f14', '')
                    if code and name and not name.startswith('*') and not name.startswith('ST'):
                        stocks.append({'code': code, 'name': name})
            return stocks
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return []

# ==================== 技术指标计算 ====================

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

def calc_macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal:
        return 0, 0, 0
    ema_fast = closes[-1]
    ema_slow = closes[-1]
    for i in range(len(closes) - 2, -1, -1):
        ema_fast = closes[i] * 2 / (fast + 1) + ema_fast * (fast - 1) / (fast + 1)
        ema_slow = closes[i] * 2 / (slow + 1) + ema_slow * (slow - 1) / (slow + 1)
    dif = ema_fast - ema_slow
    dea = dif * 2 / (signal + 1)
    bar = (dif - dea) * 2
    return dif, dea, bar

def calc_cci(highs, lows, closes, period=14):
    if len(closes) < period:
        return 0
    typical = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(len(closes))]
    sma = sum(typical[-period:]) / period
    mean_dev = sum(abs(typical[i] - sma) for i in range(len(typical) - period, len(typical))) / period
    if mean_dev == 0:
        return 0
    return (typical[-1] - sma) / (0.015 * mean_dev)

def calc_williams_r(highs, lows, closes):
    if len(closes) < 14:
        return -50
    period = 14
    high = max(highs[-period:])
    low = min(lows[-period:])
    if high == low:
        return -50
    return (high - closes[-1]) / (high - low) * -100

def calc_bollinger_position(closes, period=20, std_dev=2):
    if len(closes) < period:
        return 0.5
    ma = sum(closes[-period:]) / period
    variance = sum((c - ma) ** 2 for c in closes[-period:]) / period
    std = math.sqrt(variance)
    upper = ma + std_dev * std
    lower = ma - std_dev * std
    if upper == lower:
        return 0.5
    return (closes[-1] - lower) / (upper - lower)

def calc_psy(closes, period=12):
    if len(closes) < period + 1:
        return 50
    ups = sum(1 for i in range(1, min(period + 1, len(closes))) if closes[i] > closes[i-1])
    return ups / period * 100

def calc_mtm(closes, period=12):
    if len(closes) < period + 1:
        return 0
    return closes[-1] - closes[-period-1]

def calc_obv(closes, volumes):
    if len(closes) < 2:
        return 0
    obv = 0
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]:
            obv += volumes[i]
        elif closes[i] < closes[i-1]:
            obv -= volumes[i]
    return obv

def calc_vr(closes, volumes, period=26):
    if len(closes) < period + 1:
        return 100
    up_sum = sum(volumes[i] for i in range(len(closes) - period, len(closes)) if closes[i] >= closes[i-1])
    down_sum = sum(volumes[i] for i in range(len(closes) - period, len(closes)) if closes[i] < closes[i-1])
    if down_sum == 0:
        return 200
    return up_sum / down_sum * 100

def calc_ema(closes, period):
    if len(closes) < period:
        return closes[-1] if closes else 0
    ema = sum(closes[:period]) / period
    multiplier = 2 / (period + 1)
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
    return ema

# ==================== 因子生成 ====================

def generate_factors_for_stock(klines, lookback_ranges=[5, 10, 20, 30, 60]):
    """为单只股票生成所有候选因子"""
    if len(klines) < 70:
        return None
    
    samples = []
    
    for i in range(40, len(klines) - 5):
        try:
            closes = [klines[j]['close'] for j in range(max(0, i - 59), i + 1)]
            highs = [klines[j]['high'] for j in range(max(0, i - 59), i + 1)]
            lows = [klines[j]['low'] for j in range(max(0, i - 59), i + 1)]
            vols = [float(klines[j]['volume']) for j in range(max(0, i - 59), i + 1)]
            
            if len(closes) < 60:
                continue
            
            sample = {'code': klines[i].get('day', ''), 'day': klines[i].get('day', '')}
            
            # 价格因子
            for n in lookback_ranges:
                if i >= n:
                    pct = (closes[-1] - closes[-n-1]) / closes[-n-1] * 100 if closes[-n-1] > 0 else 0
                else:
                    pct = 0
                sample[f'pct_{n}d'] = pct
            
            # 收益率
            sample['pct_1d'] = (closes[-1] - closes[-2]) / closes[-2] * 100 if closes[-2] > 0 else 0
            
            # 波动率
            for n in [5, 10, 20, 30]:
                if len(closes) >= n + 1:
                    returns = [(closes[i] - closes[i-1]) / closes[i-1] * 100 for i in range(-n, 0)]
                    sample[f'volatility_{n}d'] = sum(returns) / n if returns else 0
                    sample[f'std_{n}d'] = (sum((r - sum(returns)/n)**2 for r in returns) / n) ** 0.5 if len(returns) > 1 else 0
                else:
                    sample[f'volatility_{n}d'] = 0
                    sample[f'std_{n}d'] = 0
            
            # RSI系列
            for period in [6, 9, 14, 26]:
                sample[f'rsi_{period}'] = calc_rsi(closes, period)
            
            # KDJ
            k, d, j = calc_kdj(highs, lows, closes)
            sample['kdj_k'] = k
            sample['kdj_d'] = d
            sample['kdj_j'] = j
            
            # MACD
            dif, dea, bar = calc_macd(closes)
            sample['macd_dif'] = dif
            sample['macd_dea'] = dea
            sample['macd_bar'] = bar
            
            # CCI
            for period in [14, 20]:
                sample[f'cci_{period}'] = calc_cci(highs, lows, closes, period)
            
            # 威廉指标
            sample['williams_r'] = calc_williams_r(highs, lows, closes)
            
            # 布林带位置
            for period in [10, 20, 30]:
                sample[f'boll_pos_{period}'] = calc_bollinger_position(closes, period)
            
            # PSY
            for period in [12, 20]:
                sample[f'psy_{period}'] = calc_psy(closes, period)
            
            # MTM动量
            for period in [3, 5, 10, 12]:
                sample[f'mtm_{period}'] = calc_mtm(closes, period)
            
            # VR
            for period in [14, 26]:
                sample[f'vr_{period}'] = calc_vr(closes, vols, period)
            
            # 均线
            for period in [5, 10, 20, 30, 60]:
                if len(closes) >= period:
                    sample[f'ma_{period}'] = sum(closes[-period:]) / period
                    sample[f'price_ma{period}'] = closes[-1] / (sum(closes[-period:]) / period) if sum(closes[-period:]) > 0 else 1
                else:
                    sample[f'ma_{period}'] = closes[-1]
                    sample[f'price_ma{period}'] = 1
            
            # EMA
            for period in [12, 26]:
                if len(closes) >= period:
                    sample[f'ema_{period}'] = calc_ema(closes, period)
                    sample[f'price_ema{period}'] = closes[-1] / calc_ema(closes, period) if calc_ema(closes, period) > 0 else 1
                else:
                    sample[f'ema_{period}'] = closes[-1]
                    sample[f'price_ema{period}'] = 1
            
            # 均线金叉死叉
            if len(closes) >= 10:
                ma5 = sum(closes[-5:]) / 5
                ma10 = sum(closes[-10:]) / 10
                ma20 = sum(closes[-20:]) / 20
                ma60 = sum(closes[-60:]) / 60 if len(closes) >= 60 else closes[-1]
                
                sample['ma5_ma10_cross'] = 1 if ma5 > ma10 else -1
                sample['ma10_ma20_cross'] = 1 if ma10 > ma20 else -1
                sample['ma20_ma60_cross'] = 1 if ma20 > ma60 else -1
                sample['ema12_ema26_cross'] = 1 if sample['ema_12'] > sample['ema_26'] else -1
            else:
                sample['ma5_ma10_cross'] = 0
                sample['ma10_ma20_cross'] = 0
                sample['ma20_ma60_cross'] = 0
                sample['ema12_ema26_cross'] = 0
            
            # 成交量比率
            for period in [5, 10, 20]:
                if len(vols) >= period + 1:
                    avg_vol = sum(vols[-period-1:-1]) / period
                    sample[f'vr_{period}'] = vols[-1] / avg_vol if avg_vol > 0 else 1
                else:
                    sample[f'vr_{period}'] = 1
            
            # OBV
            sample['obv'] = calc_obv(closes, vols)
            
            # 振幅
            if closes[-1] > 0:
                sample['amplitude'] = (highs[-1] - lows[-1]) / closes[-1] * 100
            else:
                sample['amplitude'] = 0
            
            # 高低位置
            if highs[-1] != lows[-1]:
                sample['high_low_pos'] = (closes[-1] - lows[-1]) / (highs[-1] - lows[-1])
            else:
                sample['high_low_pos'] = 0.5
            
            # 换手率估算（成交量/流通股本）
            if len(vols) >= 20:
                avg_vol20 = sum(vols[-20:]) / 20
                sample['turnover_rate_est'] = vols[-1] / avg_vol20 if avg_vol20 > 0 else 1
            else:
                sample['turnover_rate_est'] = 1
            
            # 价格动量比率
            for n in [5, 10, 20]:
                if len(closes) >= n * 2:
                    mom_curr = (closes[-1] - closes[-n-1]) / closes[-n-1] * 100
                    mom_prev = (closes[-n-1] - closes[-n*2-1]) / closes[-n*2-1] * 100 if closes[-n*2-1] > 0 else 0
                    sample[f'momentum_ratio_{n}d'] = mom_curr / (abs(mom_prev) + 0.1)
                else:
                    sample[f'momentum_ratio_{n}d'] = 1
            
            # T+1目标
            if i + 1 < len(klines):
                next_open = klines[i + 1]['open']
                next_close = klines[i + 1]['close']
                if next_open > 0:
                    sample['t1_pct'] = (next_close - next_open) / next_open * 100
                    sample['t1_up'] = 1 if sample['t1_pct'] > 0 else 0
                else:
                    sample['t1_pct'] = 0
                    sample['t1_up'] = 0
            else:
                sample['t1_pct'] = 0
                sample['t1_up'] = 0
            
            samples.append(sample)
        except Exception as e:
            continue
    
    return samples

# ==================== 因子测试 ====================

class FactorTester:
    """因子测试器"""
    
    def __init__(self):
        self.results = {}
    
    def calculate_ic(self, factor_values, target_returns):
        """计算信息系数(IC)"""
        valid_pairs = [(f, t) for f, t in zip(factor_values, target_returns) if f is not None and t is not None and not math.isnan(f) and not math.isnan(t)]
        if len(valid_pairs) < 30:
            return {'ic': 0, 'p_value': 1, 'count': len(valid_pairs)}
        
        factor_vals = [v[0] for v in valid_pairs]
        ret_vals = [v[1] for v in valid_pairs]
        
        mean_factor = sum(factor_vals) / len(factor_vals)
        mean_ret = sum(ret_vals) / len(ret_vals)
        
        cov = sum((f - mean_factor) * (r - mean_ret) for f, r in valid_pairs) / len(valid_pairs)
        std_factor = (sum((f - mean_factor) ** 2 for f in factor_vals) / len(factor_vals)) ** 0.5
        std_ret = (sum((r - mean_ret) ** 2 for r in ret_vals) / len(ret_vals)) ** 0.5
        
        if std_factor == 0 or std_ret == 0:
            return {'ic': 0, 'p_value': 1, 'count': len(valid_pairs)}
        
        ic = cov / (std_factor * std_ret)
        
        # 简化p值计算
        p_value = 0.5 - abs(ic - 0.5) if abs(ic) > 0.5 else 0.5
        
        return {'ic': ic, 'p_value': p_value, 'count': len(valid_pairs)}
    
    def quantile_backtest(self, factor_values, target_returns, quantiles=5):
        """分层回测"""
        valid_pairs = [(f, t) for f, t in zip(factor_values, target_returns) if f is not None and t is not None and not math.isnan(f) and not math.isnan(t)]
        if len(valid_pairs) < quantiles * 10:
            return None
        
        sorted_pairs = sorted(valid_pairs, key=lambda x: x[0])
        n = len(sorted_pairs)
        q_size = n // quantiles
        
        quantile_returns = []
        for i in range(quantiles):
            start = i * q_size
            end = start + q_size if i < quantiles - 1 else n
            q_returns = [p[1] for p in sorted_pairs[start:end]]
            avg_ret = sum(q_returns) / len(q_returns) if q_returns else 0
            up_ratio = len([r for r in q_returns if r > 0]) / len(q_returns) if q_returns else 0
            quantile_returns.append({
                'quantile': i + 1,
                'avg_return': avg_ret,
                'up_ratio': up_ratio,
                'count': len(q_returns)
            })
        
        # 计算多空组合收益
        long_return = quantile_returns[-1]['avg_return'] if quantile_returns else 0
        short_return = quantile_returns[0]['avg_return'] if quantile_returns else 0
        spread = long_return - short_return
        
        return {
            'quantile_returns': quantile_returns,
            'long_short_spread': spread,
            'top_bottom_spread': spread
        }
    
    def long_short_test(self, factor_values, target_returns):
        """多空组合测试"""
        valid_pairs = [(f, t) for f, t in zip(factor_values, target_returns) if f is not None and t is not None and not math.isnan(f) and not math.isnan(t)]
        if len(valid_pairs) < 30:
            return {'annual_return': 0, 'sharpe': 0}
        
        sorted_pairs = sorted(valid_pairs, key=lambda x: x[0])
        n = len(sorted_pairs)
        
        # 多头：因子值最高20%
        top_n = max(1, n // 5)
        long_returns = [p[1] for p in sorted_pairs[-top_n:]]
        short_returns = [p[1] for p in sorted_pairs[:top_n]]
        
        long_avg = sum(long_returns) / len(long_returns) if long_returns else 0
        short_avg = sum(short_returns) / len(short_returns) if short_returns else 0
        
        # 年化（假设250交易日）
        annual_return = (long_avg - short_avg) * 250
        annual_vol = (sum(long_returns) + sum(short_returns)) / len(long_returns + short_returns) * (250 ** 0.5)
        sharpe = annual_return / annual_vol if annual_vol != 0 else 0
        
        return {
            'long_return': long_avg,
            'short_return': short_avg,
            'spread': long_avg - short_avg,
            'annual_return': annual_return,
            'sharpe': sharpe
        }
    
    def out_of_sample_test(self, factor_values, target_returns, train_ratio=0.7):
        """样本外测试"""
        valid_pairs = [(f, t) for f, t in zip(factor_values, target_returns) if f is not None and t is not None and not math.isnan(f) and not math.isnan(t)]
        if len(valid_pairs) < 50:
            return {'oos_ic': 0, 'oos_return': 0}
        
        n = len(valid_pairs)
        split_idx = int(n * train_ratio)
        
        train_pairs = valid_pairs[:split_idx]
        test_pairs = valid_pairs[split_idx:]
        
        # 样本内IC
        train_ic = self.calculate_ic([p[0] for p in train_pairs], [p[1] for p in train_pairs])
        
        # 样本外IC
        test_ic = self.calculate_ic([p[0] for p in test_pairs], [p[1] for p in test_pairs])
        
        return {
            'train_ic': train_ic['ic'],
            'oos_ic': test_ic['ic'],
            'ic_decay': train_ic['ic'] - test_ic['ic']
        }


# ==================== 专业因子挖掘主类 ====================

class ProfessionalFactorMiner:
    """专业因子挖掘器"""
    
    def __init__(self, output_dir='/mnt/d/AStockV4/predictions/factor_library'):
        self.output_dir = output_dir
        self.factor_tester = FactorTester()
        self.factor_library = []
        self.candidate_results = []
        os.makedirs(output_dir, exist_ok=True)
    
    def collect_stock_data(self, stock_list, max_stocks=500, days=500):
        """采集股票数据"""
        all_samples = []
        collected = 0
        
        print(f"\n开始采集 {min(max_stocks, len(stock_list))} 只股票数据...")
        
        for i, stock in enumerate(stock_list[:max_stocks]):
            code = stock['code']
            name = stock['name']
            
            # 使用腾讯接口获取K线
            klines = get_kline_tencent(code, datalen=days)
            
            if klines and len(klines) > 60:
                samples = generate_factors_for_stock(klines)
                if samples:
                    for s in samples:
                        s['stock_code'] = code
                        s['stock_name'] = name
                    all_samples.extend(samples)
                    collected += 1
            
            if (i + 1) % 50 == 0:
                print(f"  进度: {i+1}/{min(max_stocks, len(stock_list))}  已采集: {collected} 只  样本: {len(all_samples)}")
            
            time.sleep(0.1)
        
        print(f"\n采集完成: {collected} 只股票, {len(all_samples)} 样本")
        return all_samples
    
    def test_all_factors(self, samples):
        """测试所有候选因子"""
        if not samples or len(samples) < 100:
            print("样本量不足!")
            return []
        
        # 获取所有因子名称（排除非因子字段）
        exclude_fields = {'code', 'day', 'stock_code', 'stock_name', 't1_pct', 't1_up'}
        factor_names = [k for k in samples[0].keys() if k not in exclude_fields]
        
        target_returns = [s['t1_pct'] for s in samples]
        target_binary = [s['t1_up'] for s in samples]
        
        print(f"\n开始测试 {len(factor_names)} 个因子...")
        
        results = []
        
        for fname in factor_names:
            try:
                factor_values = [s.get(fname) for s in samples]
                
                # IC分析
                ic_result = self.factor_tester.calculate_ic(factor_values, target_returns)
                
                # 分层回测
                quantile_result = self.factor_tester.quantile_backtest(factor_values, target_returns)
                
                # 多空组合
                long_short = self.factor_tester.long_short_test(factor_values, target_returns)
                
                # 样本外测试
                oos = self.factor_tester.out_of_sample_test(factor_values, target_returns)
                
                result = {
                    'factor_name': fname,
                    'ic': ic_result['ic'],
                    'ic_abs': abs(ic_result['ic']),
                    'p_value': ic_result['p_value'],
                    'count': ic_result['count'],
                    'long_short_spread': long_short['spread'] if long_short else 0,
                    'annual_return': long_short['annual_return'] if long_short else 0,
                    'sharpe': long_short['sharpe'] if long_short else 0,
                    'oos_ic': oos['oos_ic'] if oos else 0,
                    'train_ic': oos['train_ic'] if oos else 0,
                    'ic_decay': oos['ic_decay'] if oos else 0,
                    'quantile_returns': quantile_result['quantile_returns'] if quantile_result else []
                }
                
                results.append(result)
                
            except Exception as e:
                continue
        
        # 按IC排序
        results.sort(key=lambda x: -x['ic_abs'])
        
        print(f"测试完成: {len(results)} 个因子")
        return results
    
    def filter_effective_factors(self, results, thresholds=None):
        """筛选有效因子"""
        if thresholds is None:
            thresholds = {
                'ic_abs_min': 0.02,      # IC绝对值 > 0.02
                'count_min': 100,          # 样本数 > 100
                'annual_return_min': 0.05, # 年化收益 > 5%
                'sharpe_min': 0.3,         # 夏普比率 > 0.3
                'oos_ic_min': 0.01        # 样本外IC > 0.01
            }
        
        effective = []
        marginal = []
        rejected = []
        
        for r in results:
            if r['count'] < thresholds['count_min']:
                rejected.append(r)
                continue
            
            score = 0
            if r['ic_abs'] > thresholds['ic_abs_min']:
                score += 1
            if r['annual_return'] > thresholds['annual_return_min']:
                score += 1
            if r['sharpe'] > thresholds['sharpe_min']:
                score += 1
            if r['oos_ic'] > thresholds['oos_ic_min']:
                score += 1
            
            r['effective_score'] = score
            
            if score >= 3:
                effective.append(r)
            elif score >= 2:
                marginal.append(r)
            else:
                rejected.append(r)
        
        print(f"\n有效因子: {len(effective)} 个")
        print(f"边缘因子: {len(marginal)} 个")
        print(f"淘汰因子: {len(rejected)} 个")
        
        return effective, marginal, rejected
    
    def remove_correlated_factors(self, effective_factors, correlation_threshold=0.7):
        """去除高度相关的因子"""
        if len(effective_factors) <= 1:
            return effective_factors
        
        selected = []
        
        for ef in effective_factors:
            is_redundant = False
            for sel in selected:
                # 简化的相关性判断 - 实际应该计算相关系数
                if (ef['factor_name'].startswith(sel['factor_name'][:5]) or 
                    sel['factor_name'].startswith(ef['factor_name'][:5])):
                    # 如果已选因子IC更高，保留已选的
                    if sel['ic_abs'] >= ef['ic_abs']:
                        is_redundant = True
                        break
                    else:
                        # 如果新因子IC更高，替换
                        selected.remove(sel)
                        break
            
            if not is_redundant:
                selected.append(ef)
        
        print(f"\n去冗余后有效因子: {len(selected)} 个")
        return selected
    
    def run(self, max_stocks=500):
        """运行完整流程"""
        print("=" * 80)
        print("【Professional Factor Mining System - B系统】")
        print("=" * 80)
        
        # Step 1: 获取股票列表
        print("\n[Step 1] 获取A股股票列表...")
        stock_list = get_stock_list_a_share()
        print(f"获取到 {len(stock_list)} 只股票")
        
        if not stock_list:
            print("获取股票列表失败!")
            return None
        
        # Step 2: 采集数据
        samples = self.collect_stock_data(stock_list, max_stocks=max_stocks)
        
        if not samples:
            print("采集数据失败!")
            return None
        
        # 保存原始样本
        samples_file = os.path.join(self.output_dir, 'raw_samples.json')
        with open(samples_file, 'w') as f:
            json.dump(samples[:50000], f, ensure_ascii=False)
        print(f"原始样本已保存: {samples_file}")
        
        # Step 3: 测试所有因子
        results = self.test_all_factors(samples)
        
        if not results:
            print("因子测试失败!")
            return None
        
        # 保存测试结果
        results_file = os.path.join(self.output_dir, 'factor_test_results.json')
        with open(results_file, 'w') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"测试结果已保存: {results_file}")
        
        # Step 4: 筛选有效因子
        effective, marginal, rejected = self.filter_effective_factors(results)
        
        # Step 5: 去冗余
        selected = self.remove_correlated_factors(effective)
        
        # Step 6: 保存最终因子库
        factor_library = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'total_samples': len(samples),
            'stocks_tested': max_stocks,
            'total_factors_tested': len(results),
            'effective_factors': len(effective),
            'selected_factors': len(selected),
            'factor_library': selected[:50]  # 最多保留50个
        }
        
        library_file = os.path.join(self.output_dir, 'factor_library.json')
        with open(library_file, 'w') as f:
            json.dump(factor_library, f, ensure_ascii=False, indent=2)
        print(f"因子库已保存: {library_file}")
        
        # Step 7: 打印TOP因子
        print("\n" + "=" * 80)
        print("【TOP 30 有效因子】")
        print("=" * 80)
        print(f"{'排名':<4} {'因子名称':<25} {'IC':<10} {'IC_IR':<10} {'年化收益':<12} {'夏普':<8} {'样本外IC':<10}")
        print("-" * 90)
        
        for i, r in enumerate(results[:30], 1):
            print(f"{i:<4} {r['factor_name']:<25} {r['ic']:>8.4f} {r['ic']/max(r['sharpe'],0.1):>10.4f} {r['annual_return']:>10.2%} {r['sharpe']:>8.2f} {r['oos_ic']:>10.4f}")
        
        print("\n" + "=" * 80)
        print("【优化完成】")
        print("=" * 80)
        
        return factor_library


# ==================== 主程序 ====================

if __name__ == '__main__':
    miner = ProfessionalFactorMiner()
    
    # 测试模式：先用100只股票
    library = miner.run(max_stocks=500)
    
    if library:
        print(f"\n因子库包含 {library['selected_factors']} 个精选因子")
        print(f"基于 {library['total_samples']} 个样本")
        print(f"测试了 {library['stocks_tested']} 只股票")