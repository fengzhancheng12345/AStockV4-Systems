"""
V4.5系统 - 基于20年回测的因子挖掘更新版
V38.2 + 深度因子规则V4.5 + 市场环境自适应

更新内容（基于47,267样本回测）：
1. 新增RSI6、KDJ_J、威廉指标、PSY、布林带位置等因子
2. 更新评分规则为回测验证过的准确率
3. 新增最佳组合：RSI55-65+跌幅10-6% → 准确率73.6%
4. 新增规避规则：RSI45-55+涨幅>6% → 准确率36.8%
"""

import json
import urllib.request
import time
from datetime import datetime
from typing import List, Dict, Optional

from technical_factors import TechnicalFactors


class V45ExperienceFactors:
    """
    V4.5深度因子规则
    基于20年47,267样本回测验证
    """
    
    @staticmethod
    def calc_rsi6(closes: List[float]) -> float:
        """计算RSI6"""
        if len(closes) < 7:
            return 50.0
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains[-6:]) / 6
        avg_loss = sum(losses[-6:]) / 6
        if avg_loss == 0:
            return 100.0
        return 100 - (100 / (1 + avg_gain / avg_loss))
    
    @staticmethod
    def calc_williams_r(high: float, low: float, close: float) -> float:
        """计算威廉指标"""
        if high == low:
            return -50
        return (high - close) / (high - low) * -100
    
    @staticmethod
    def calc_psy(closes: List[float]) -> float:
        """计算PSY心理线"""
        if len(closes) < 12:
            return 50
        ups = sum(1 for i in range(1, min(12, len(closes))) if closes[i] > closes[i-1])
        return ups / 12 * 100
    
    @staticmethod
    def calc_boll_position(closes: List[float]) -> float:
        """计算布林带位置"""
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
    
    @staticmethod
    def calc_price_ma60_ratio(close: float, ma60: float) -> float:
        """计算价格/MA60比值"""
        return close / ma60 if ma60 > 0 else 1.0
    
    def comprehensive_score(self, factors: Dict) -> tuple:
        """
        V4.5综合评分
        返回: (评分, 理由列表, 置信度, 信号强度)
        """
        score = 0
        reasons = []
        signals = []
        
        rsi14 = factors.get('rsi_14', 50)
        rsi6 = factors.get('rsi_6', 50)
        vr = factors.get('vr', 1)
        pct = factors.get('change_pct', 0)
        pct5 = factors.get('change_5d', 0)
        pct10 = factors.get('change_10d', 0)
        pct60 = factors.get('change_60d', 0)
        kdj_j = factors.get('kdj_j', 50)
        williams_r = factors.get('williams_r', -50)
        psy = factors.get('psy', 50)
        boll_pos = factors.get('boll_position', 0.5)
        price_ma60 = factors.get('price_ma60', 1.0)
        
        # ========== 核心买入信号（基于回测高分组合） ==========
        
        # 信号1: RSI55-65 + 跌幅10-6% → 准确率73.6% ⭐⭐⭐最强
        if 55 <= rsi14 <= 65 and -10 <= pct < -6:
            score += 20
            reasons.append("RSI55-65+跌幅10-6% [准确率73.6%]")
            signals.append(('STRONG_BUY', 20))
        
        # 信号2: RSI<25 + 跌幅10-6% → 准确率62.8%
        elif rsi14 < 25 and -10 <= pct < -6:
            score += 15
            reasons.append("RSI<25+跌幅10-6% [准确率62.8%]")
            signals.append(('BUY', 15))
        
        # 信号3: RSI45-55 + 跌幅10-6% → 准确率58.1%
        elif 45 <= rsi14 < 55 and -10 <= pct < -6:
            score += 14
            reasons.append("RSI45-55+跌幅10-6% [准确率58.1%]")
            signals.append(('BUY', 14))
        
        # 信号4: RSI25-35 + 跌幅10-6% → 准确率58.1%
        elif 25 <= rsi14 < 35 and -10 <= pct < -6:
            score += 14
            reasons.append("RSI25-35+跌幅10-6% [准确率58.1%]")
            signals.append(('BUY', 14))
        
        # 信号5: RSI35-45 + 跌幅10-6% → 准确率56.4%
        elif 35 <= rsi14 < 45 and -10 <= pct < -6:
            score += 13
            reasons.append("RSI35-45+跌幅10-6% [准确率56.4%]")
            signals.append(('BUY', 13))
        
        # 信号6: 10日跌幅20-15% → 准确率60.6%
        if -20 <= pct10 < -15:
            score += 12
            reasons.append("10日跌幅20-15% [准确率60.6%]")
            signals.append(('BUY', 12))
        
        # 信号7: 10日跌幅<-20% → 准确率58.4%
        if pct10 < -20:
            score += 11
            reasons.append("10日跌幅>20% [准确率58.4%]")
            signals.append(('BUY', 11))
        
        # 信号8: 10日跌幅15-10% → 准确率55%
        if -15 <= pct10 < -10:
            score += 10
            reasons.append("10日跌幅15-10% [准确率55%]")
            signals.append(('BUY', 10))
        
        # 信号9: KDJ_J<0 + RSI<30 → 准确率54.8%
        if kdj_j < 0 and rsi14 < 30:
            score += 10
            reasons.append("KDJ_J<0+RSI<30双重超卖 [准确率54.8%]")
            signals.append(('BUY', 10))
        
        # 信号10: 威廉<-80 + RSI>70 → 准确率56.5%（低位+高位矛盾）
        if williams_r < -80 and rsi14 > 70:
            score += 9
            reasons.append("威廉<-80+RSI>70极端背离")
            signals.append(('WARN', 9))
        
        # 信号11: 布林带<0.15 + RSI<30 → 准确率54.7%
        if boll_pos < 0.15 and rsi14 < 30:
            score += 9
            reasons.append("布林下轨+RSI超卖 [准确率54.7%]")
            signals.append(('BUY', 9))
        
        # 信号12: 价格/MA60<0.7 → 准确率57.3%
        if price_ma60 < 0.7:
            score += 11
            reasons.append("价格严重低于MA60 [准确率57.3%]")
            signals.append(('BUY', 11))
        
        # 信号13: 价格/MA60 0.7-0.8 → 准确率55.5%
        elif 0.7 <= price_ma60 < 0.8:
            score += 8
            reasons.append("价格低于MA60 20-30% [准确率55.5%]")
            signals.append(('BUY', 8))
        
        # 信号14: RSI6<20 → 准确率53%
        if rsi6 < 20:
            score += 6
            reasons.append("RSI6<20短期超卖")
            signals.append(('BUY', 6))
        
        # 信号15: 60日跌幅60-40% → 准确率54.8%
        if -60 <= pct60 < -40:
            score += 8
            reasons.append("60日跌幅40-60%")
            signals.append(('BUY', 8))
        
        # 信号16: RSI65-75 + 跌幅6-2% → 准确率57.4%
        if 65 <= rsi14 <= 75 and -6 <= pct < -2:
            score += 10
            reasons.append("RSI65-75+温和下跌 [准确率57.4%]")
            signals.append(('BUY', 10))
        
        # 信号17: RSI<25 + 涨幅2-6% → 准确率59.5%（超跌反弹进行中）
        if rsi14 < 25 and 2 <= pct < 6:
            score += 12
            reasons.append("RSI<25+反弹进行中 [准确率59.5%]")
            signals.append(('BUY', 12))
        
        # ========== 辅助加分 ==========
        
        # 威廉<-80低位
        if williams_r < -80:
            score += 4
            reasons.append("威廉指标极度超卖")
        
        # KDJ_J<0
        if kdj_j < 0:
            score += 3
            reasons.append("KDJ_J低位")
        
        # 布林下轨
        if boll_pos < 0.1:
            score += 4
            reasons.append("布林带下轨")
        
        # ========== 规避信号（基于回测低分组合） ==========
        
        avoid_score = 0
        avoid_reasons = []
        
        # 规避1: RSI45-55 + 涨幅>6% → 准确率36.8% ❌
        if 45 <= rsi14 <= 55 and pct > 6:
            avoid_score += 15
            avoid_reasons.append("RSI45-55+涨幅>6% [准确率36.8%规避!]")
        
        # 规避2: PSY>85 → 准确率34% ❌❌
        if psy > 85:
            avoid_score += 18
            avoid_reasons.append("PSY>85极度乐观 [准确率34%离场!]")
        
        # 规避3: KDJ_J>100 → 准确率43.8%
        if kdj_j > 100:
            avoid_score += 8
            avoid_reasons.append("KDJ_J>100高位钝化")
        
        # 规避4: RSI>75 + 涨幅>6% → 准确率43.6%
        if rsi14 > 75 and pct > 6:
            avoid_score += 10
            avoid_reasons.append("RSI高位+涨幅过大")
        
        # 规避5: RSI>75 + 涨幅2-6% → 准确率43.3%
        if rsi14 > 75 and 2 <= pct < 6:
            avoid_score += 8
            avoid_reasons.append("RSI高位+温和上涨")
        
        # 规避6: 布林上轨+高位
        if boll_pos > 0.9 and rsi14 > 65:
            avoid_score += 7
            avoid_reasons.append("布林上轨+RSI高位")
        
        # 规避7: PSY>75 → 准确率46.6%
        if psy > 75:
            avoid_score += 5
            avoid_reasons.append("PSY>75过度乐观")
        
        # 最终评分 = 正向分数 - 规避分数
        final_score = score - avoid_score
        
        # 合并理由
        all_reasons = reasons + avoid_reasons
        
        # 置信度评估
        confidence = 0.5
        if abs(final_score) >= 20:
            confidence = 0.85
        elif abs(final_score) >= 15:
            confidence = 0.75
        elif abs(final_score) >= 10:
            confidence = 0.65
        elif abs(final_score) >= 5:
            confidence = 0.55
        
        # 信号方向
        if final_score >= 15:
            direction = '强烈推荐'
        elif final_score >= 8:
            direction = '谨慎推荐'
        elif final_score <= -10:
            direction = '强烈规避'
        elif final_score <= -5:
            direction = '建议规避'
        else:
            direction = '观望'
        
        return final_score, all_reasons, confidence, direction, signals


class V45Analyzer:
    """V4.5综合分析器"""
    
    def __init__(self):
        self.tech = TechnicalFactors()
        self.v45 = V45ExperienceFactors()
    
    def get_kl_sina(self, code: str, count: int = 120) -> List[Dict]:
        """获取新浪K线（更长）"""
        sym = f'sh{code}' if code.startswith(('6', '9')) else f'sz{code}'
        url = f'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={sym}&scale=240&ma=no&datalen={count}'
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read().decode('utf-8', errors='ignore'))
                if not data:
                    return []
                return [{'day': d['day'], 'open': float(d['open']), 'high': float(d['high']), 
                        'low': float(d['low']), 'close': float(d['close']), 'volume': float(d['volume'])} 
                       for d in data]
        except:
            return []
    
    def calculate_v45_factors(self, code: str) -> Optional[Dict]:
        """计算V4.5所有因子"""
        kl = self.get_kl_sina(code, 120)
        if len(kl) < 65:
            return None
        
        closes = [k['close'] for k in kl]
        highs = [k['high'] for k in kl]
        lows = [k['low'] for k in kl]
        
        factors = {
            'code': code,
            'day': kl[-1]['day'],
            'close': closes[-1],
            'change_pct': (closes[-1] - closes[-2]) / closes[-2] * 100 if len(closes) >= 2 else 0,
            'change_5d': (closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0,
            'change_10d': (closes[-1] - closes[-11]) / closes[-11] * 100 if len(closes) >= 11 else 0,
            'change_60d': (closes[-1] - closes[-61]) / closes[-61] * 100 if len(closes) >= 61 else 0,
            
            # RSI
            'rsi_14': self.tech.calc_rsi(closes, 14),
            'rsi_6': self.v45.calc_rsi6(closes),
            
            # KDJ
            'kdj': self.tech.calc_kdj(highs, lows, closes),
            'kdj_j': self.tech.calc_kdj(highs, lows, closes)['j'],
            
            # 威廉指标
            'williams_r': self.v45.calc_williams_r(highs[-1], lows[-1], closes[-1]),
            
            # PSY
            'psy': self.v45.calc_psy(closes),
            
            # 布林带位置
            'boll_position': self.v45.calc_boll_position(closes),
            
            # 量比
            'vr': self.tech.calc_vol_ratio(kl),
            
            # 价格/MA60
            'ma60': self.tech.calc_ma(closes, 60) if len(closes) >= 60 else closes[-1],
            'price_ma60': self.v45.calc_price_ma60_ratio(closes[-1], 
                                                          self.tech.calc_ma(closes, 60) if len(closes) >= 60 else closes[-1]),
            
            # MA
            'ma5': self.tech.calc_ma(closes, 5),
            'ma20': self.tech.calc_ma(closes, 20),
        }
        
        return factors
    
    def get_all_stocks(self) -> List[Dict]:
        """获取全市场股票"""
        stocks = []
        for node in ['hs_a']:
            for page in range(1, 15):
                url = f'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeDataSimple?page={page}&num=50&sort=amount&asc=0&node={node}'
                try:
                    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn/'})
                    with urllib.request.urlopen(req, timeout=10) as r:
                        data = json.loads(r.read().decode('utf-8', errors='ignore'))
                    if not data:
                        break
                    for s in data:
                        code = s.get('code', '')
                        name = s.get('name', '')
                        pct = float(s.get('changepercent', 0))
                        price = float(s.get('trade', 0))
                        if any(kw in name for kw in ['ST', '*ST', '退', 'N']) or price <= 0 or price > 500:
                            continue
                        if code.startswith(('688', '300')):
                            continue
                        stocks.append({'code': code, 'name': name, 'pct': pct, 'price': price})
                except:
                    break
                time.sleep(0.05)
        return stocks
    
    def analyze_stocks(self, stocks: List[Dict]) -> List[Dict]:
        """分析所有股票"""
        results = []
        
        for i, s in enumerate(stocks):
            code = s['code']
            
            factors = self.calculate_v45_factors(code)
            if not factors:
                continue
            
            score, reasons, confidence, direction, signals = self.v45.comprehensive_score(factors)
            
            results.append({
                'code': code,
                'name': s['name'],
                'price': s['price'],
                'pct': s['pct'],
                'rsi_14': factors['rsi_14'],
                'rsi_6': factors['rsi_6'],
                'kdj_j': factors['kdj_j'],
                'williams_r': factors['williams_r'],
                'psy': factors['psy'],
                'boll_position': factors['boll_position'],
                'vr': factors['vr'],
                'price_ma60': factors['price_ma60'],
                'change_10d': factors['change_10d'],
                'score': score,
                'reasons': reasons[:3],
                'confidence': confidence,
                'direction': direction
            })
            
            if (i + 1) % 100 == 0:
                print(f"  {i+1}/{len(stocks)} 完成")
            
            time.sleep(0.03)
        
        return results
    
    def filter_signals(self, results: List[Dict]) -> tuple:
        """过滤信号"""
        # 强烈推荐
        strong_buy = [r for r in results if r['direction'] == '强烈推荐']
        strong_buy.sort(key=lambda x: -x['score'])
        
        # 谨慎推荐
        cautious_buy = [r for r in results if r['direction'] == '谨慎推荐']
        cautious_buy.sort(key=lambda x: -x['score'])
        
        # 强烈规避
        strong_avoid = [r for r in results if r['direction'] == '强烈规避']
        strong_avoid.sort(key=lambda x: x['score'])
        
        return strong_buy, cautious_buy, strong_avoid


def main():
    print("=" * 80)
    print("V4.5系统 - 基于20年回测因子挖掘更新版")
    print("=" * 80)
    
    analyzer = V45Analyzer()
    date = datetime.now().strftime('%Y-%m-%d')
    
    # 1. 获取股票池
    print("\n【1. 获取股票池】")
    stocks = analyzer.get_all_stocks()
    print(f"  获取到 {len(stocks)} 只股票")
    
    # 2. 分析
    print("\n【2. V4.5因子分析】")
    results = analyzer.analyze_stocks(stocks)
    print(f"  分析完成: {len(results)} 只")
    
    # 3. 过滤信号
    print("\n【3. 信号过滤】")
    strong_buy, cautious_buy, strong_avoid = analyzer.filter_signals(results)
    print(f"  强烈推荐: {len(strong_buy)} 只")
    print(f"  谨慎推荐: {len(cautious_buy)} 只")
    print(f"  强烈规避: {len(strong_avoid)} 只")
    
    # 4. 显示结果
    print("\n" + "=" * 80)
    print(f"【{date} V4.5系统分析报告】")
    print("=" * 80)
    
    print("\n" + "-" * 80)
    print("【强烈推荐 TOP 15】")
    print("-" * 80)
    print(f"{'排名':<4} {'股票':<10} {'价':<8} {'涨跌幅':<8} {'RSI14':<6} {'RSI6':<6} {'KDJ_J':<7} {'威廉':<7} {'BOLL':<6} {'评分':<6} {'核心信号'}")
    print("-" * 80)
    
    for i, r in enumerate(strong_buy[:15], 1):
        reason = r['reasons'][0][:15] if r['reasons'] else ''
        print(f"{i:<4} {r['name']:<10} {r['price']:<8.2f} {r['pct']:>+6.2f}% {r['rsi_14']:<6.0f} {r['rsi_6']:<6.0f} {r['kdj_j']:<7.0f} {r['williams_r']:<7.0f} {r['boll_position']:<6.2f} {r['score']:<+6.1f} {reason}")
    
    if cautious_buy:
        print("\n" + "-" * 80)
        print("【谨慎推荐 TOP 10】")
        print("-" * 80)
        print(f"{'排名':<4} {'股票':<10} {'价':<8} {'涨跌幅':<8} {'RSI14':<6} {'RSI6':<6} {'KDJ_J':<7} {'评分':<6} {'核心信号'}")
        print("-" * 80)
        
        for i, r in enumerate(cautious_buy[:10], 1):
            reason = r['reasons'][0][:15] if r['reasons'] else ''
            print(f"{i:<4} {r['name']:<10} {r['price']:<8.2f} {r['pct']:>+6.2f}% {r['rsi_14']:<6.0f} {r['rsi_6']:<6.0f} {r['kdj_j']:<7.0f} {r['score']:<+6.1f} {reason}")
    
    if strong_avoid:
        print("\n" + "-" * 80)
        print("【强烈规避 TOP 10】")
        print("-" * 80)
        print(f"{'排名':<4} {'股票':<10} {'价':<8} {'涨跌幅':<8} {'RSI14':<6} {'RSI6':<6} {'PSY':<6} {'评分':<6} {'核心风险'}")
        print("-" * 80)
        
        for i, r in enumerate(strong_avoid[:10], 1):
            reason = r['reasons'][0][:15] if r['reasons'] else ''
            print(f"{i:<4} {r['name']:<10} {r['price']:<8.2f} {r['pct']:>+6.2f}% {r['rsi_14']:<6.0f} {r['rsi_6']:<6.0f} {r['psy']:<6.0f} {r['score']:<+6.1f} {reason}")
    
    # 5. 保存结果
    report = {
        'date': date,
        'total_analyzed': len(results),
        'strong_buy': strong_buy[:30],
        'cautious_buy': cautious_buy[:20],
        'strong_avoid': strong_avoid[:20],
        'summary': {
            'strong_buy_count': len(strong_buy),
            'cautious_buy_count': len(cautious_buy),
            'strong_avoid_count': len(strong_avoid)
        }
    }
    
    output_file = f'/mnt/d/AStockV4/predictions/v45_analysis_{date.replace("-", "")}.json'
    with open(output_file, 'w') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n报告已保存: {output_file}")
    
    # 6. 更新经验规则文件
    update_experience_rules()
    
    return report


def update_experience_rules():
    """更新经验规则文件"""
    rules = {
        "version": "V4.5",
        "update_date": datetime.now().strftime('%Y-%m-%d'),
        "sample_count": 47267,
        "best_combinations": [
            {"rank": 1, "condition": "RSI55-65 + 跌幅10-6%", "accuracy": 73.6, "avg_return": 1.52, "score": 20},
            {"rank": 2, "condition": "RSI<25 + 跌幅10-6%", "accuracy": 62.8, "avg_return": 1.10, "score": 15},
            {"rank": 3, "condition": "RSI<25 + 涨幅2-6%", "accuracy": 59.5, "avg_return": 0.87, "score": 12},
            {"rank": 4, "condition": "RSI45-55 + 跌幅10-6%", "accuracy": 58.1, "avg_return": 1.06, "score": 14},
            {"rank": 5, "condition": "RSI25-35 + 跌幅10-6%", "accuracy": 58.1, "avg_return": 0.93, "score": 14},
            {"rank": 6, "condition": "RSI65-75 + 跌幅6-2%", "accuracy": 57.4, "avg_return": 0.49, "score": 10},
            {"rank": 7, "condition": "10日跌幅20-15%", "accuracy": 60.6, "avg_return": 0.54, "score": 12},
            {"rank": 8, "condition": "价格/MA60<0.7", "accuracy": 57.3, "avg_return": 0.65, "score": 11},
            {"rank": 9, "condition": "KDJ_J<0 + RSI<30", "accuracy": 54.8, "avg_return": 0.18, "score": 10},
            {"rank": 10, "condition": "威廉<-80 + RSI>70", "accuracy": 56.5, "avg_return": 0.46, "score": 9},
        ],
        "avoid_combinations": [
            {"rank": 1, "condition": "RSI45-55 + 涨幅>6%", "accuracy": 36.8, "risk": "追高陷阱", "penalty": 15},
            {"rank": 2, "condition": "PSY>85", "accuracy": 34.0, "risk": "极度离场信号", "penalty": 18},
            {"rank": 3, "condition": "KDJ_J>100", "accuracy": 43.8, "risk": "高位钝化", "penalty": 8},
            {"rank": 4, "condition": "RSI>75 + 涨幅>6%", "accuracy": 43.6, "risk": "高位追涨", "penalty": 10},
            {"rank": 5, "condition": "RSI>75 + 涨幅2-6%", "accuracy": 43.3, "risk": "高位微涨", "penalty": 8},
        ],
        "new_factors": [
            {"name": "RSI6", "description": "短期RSI,<20超卖,>80超买"},
            {"name": "KDJ_J", "description": "KDJ的J值,<0超卖,>100高位钝化"},
            {"name": "威廉指标", "description": "<-80极度超卖,>-10极度超买"},
            {"name": "PSY", "description": "心理线,>85极度乐观离场,<25极度悲观"},
            {"name": "布林带位置", "description": "<0.1下轨买入,>0.9上轨卖出"},
            {"name": "价格/MA60", "description": "<0.7严重超跌,>1.3严重超买"},
            {"name": "10日涨跌幅", "description": "<-20%超跌反弹信号"},
        ]
    }
    
    with open('/mnt/d/AStockV4/predictions/v45_experience_rules.json', 'w') as f:
        json.dump(rules, f, ensure_ascii=False, indent=2)
    
    print(f"\n经验规则已更新: /mnt/d/AStockV4/predictions/v45_experience_rules.json")


if __name__ == '__main__':
    main()