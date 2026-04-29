"""
V4.5系统 - 技术因子库
Technical Factors Module

基于20年47,267样本回测验证
新增: RSI6, KDJ_J, 威廉指标, PSY, 布林带位置, 价格/MA60比率
"""

import json
import urllib.request
import time
from typing import List, Dict, Optional


class TechnicalFactors:
    """技术面因子计算"""
    
    @staticmethod
    def get_kl(code: str, count: int = 60) -> List[Dict]:
        """获取K线数据"""
        sym = f'sh{code}' if code.startswith(('6', '9')) else f'sz{code}'
        url = f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_dayqfq&param={sym},day,,,{count},qfq'
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://gu.qq.com/'})
            with urllib.request.urlopen(req, timeout=8) as r:
                raw = r.read().decode('utf-8', errors='ignore')
                data = json.loads(raw[raw.find('=') + 1:])
                sym_data = data.get('data', {}).get(sym, {})
                days = sym_data.get('qfqday', []) or sym_data.get('day', [])
                return [{'day': d[0], 'open': float(d[1]), 'close': float(d[2]), 
                        'high': float(d[3]), 'low': float(d[4]), 'volume': float(d[5])} 
                       for d in days if len(d) >= 6]
        except:
            return []
    
    @staticmethod
    def get_kl_sina(code: str, count: int = 120) -> List[Dict]:
        """获取新浪K线（更长周期）"""
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
    
    @staticmethod
    def calc_rsi(closes: List[float], period: int = 14) -> float:
        """计算RSI"""
        if len(closes) < period + 1:
            return 50.0
        gains = []
        losses = []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))
    
    @staticmethod
    def calc_rsi6(closes: List[float]) -> float:
        """计算RSI6短期RSI"""
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
    def calc_macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Dict:
        """计算MACD"""
        if len(closes) < slow:
            return {'macd': 0, 'signal': 0, 'histogram': 0}
        
        def calc_ema(data, period):
            ema = [data[0]]
            multiplier = 2 / (period + 1)
            for i in range(1, len(data)):
                ema.append((data[i] - ema[-1]) * multiplier + ema[-1])
            return ema
        
        ema_fast = calc_ema(closes, fast)
        ema_slow = calc_ema(closes, slow)
        
        dif = [ema_fast[i] - ema_slow[i] for i in range(len(closes))]
        dea = calc_ema(dif, signal)
        
        macd = dif[-1] * 2
        signal_line = dea[-1]
        histogram = macd - signal_line
        
        return {'macd': macd, 'signal': signal_line, 'histogram': histogram}
    
    @staticmethod
    def calc_kdj(highs: List[float], lows: List[float], closes: List[float], period: int = 9) -> Dict:
        """计算KDJ"""
        if len(closes) < period:
            return {'k': 50, 'd': 50, 'j': 50}
        
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
        
        return {'k': k, 'd': d, 'j': j}
    
    @staticmethod
    def calc_ma(closes: List[float], period: int) -> float:
        """计算MA"""
        if len(closes) < period:
            return closes[-1] if closes else 0
        return sum(closes[-period:]) / period
    
    @staticmethod
    def calc_vol_ratio(kl: List[Dict], idx: int = -1) -> float:
        """计算量比"""
        if idx < 5 or idx >= len(kl):
            return 1.0
        vols = [kl[i]['volume'] for i in range(idx - 5, idx)]
        avg_vol = sum(vols) / 5
        today_vol = kl[idx]['volume']
        return today_vol / avg_vol if avg_vol > 0 else 1.0
    
    @staticmethod
    def calc_bias(closes: List[float], periods: List[int] = [5, 10, 20]) -> Dict:
        """计算乖离率"""
        result = {}
        for period in periods:
            ma = TechnicalFactors.calc_ma(closes, period)
            if ma > 0:
                bias = (closes[-1] - ma) / ma * 100
            else:
                bias = 0
            result[f'bias_{period}'] = bias
        return result
    
    @staticmethod
    def calc_bollinger(closes: List[float], period: int = 20, std_dev: int = 2) -> Dict:
        """计算布林带"""
        if len(closes) < period:
            return {'upper': 0, 'middle': 0, 'lower': 0, 'bandwidth': 0}
        
        ma = sum(closes[-period:]) / period
        variance = sum((c - ma) ** 2 for c in closes[-period:]) / period
        std = variance ** 0.5
        
        upper = ma + std_dev * std
        lower = ma - std_dev * std
        bandwidth = (upper - lower) / ma * 100 if ma > 0 else 0
        
        return {'upper': upper, 'middle': ma, 'lower': lower, 'bandwidth': bandwidth}
    
    @staticmethod
    def calc_momentum(closes: List[float], period: int = 10) -> float:
        """计算动量"""
        if len(closes) < period + 1:
            return 0
        return closes[-1] - closes[-period - 1]
    
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
        """
        计算布林带位置
        返回0-1之间的值，<0.1为下轨，>0.9为上轨
        """
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
    
    def calculate_all_factors(self, code: str, use_long_kl: bool = False) -> Optional[Dict]:
        """计算某只股票的所有技术因子"""
        if use_long_kl:
            kl = self.get_kl_sina(code, 120)
        else:
            kl = self.get_kl(code, 60)
        
        if len(kl) < 30:
            return None
        
        closes = [k['close'] for k in kl]
        highs = [k['high'] for k in kl]
        lows = [k['low'] for k in kl]
        
        kdj = self.calc_kdj(highs, lows, closes)
        bollinger = self.calc_bollinger(closes)
        
        factors = {
            'code': code,
            'name': kl[-1]['day'],
            # 基础价格数据
            'close': closes[-1],
            'change_pct': (closes[-1] - closes[-2]) / closes[-2] * 100 if len(closes) >= 2 else 0,
            'change_5d': (closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0,
            'change_10d': (closes[-1] - closes[-11]) / closes[-11] * 100 if len(closes) >= 11 else 0,
            'change_20d': (closes[-1] - closes[-21]) / closes[-21] * 100 if len(closes) >= 21 else 0,
            
            # RSI (多种周期)
            'rsi_6': self.calc_rsi6(closes),
            'rsi_14': self.calc_rsi(closes, 14),
            'rsi_26': self.calc_rsi(closes, 26),
            
            # MACD
            'macd': self.calc_macd(closes),
            
            # KDJ
            'kdj': kdj,
            'kdj_k': kdj['k'],
            'kdj_d': kdj['d'],
            'kdj_j': kdj['j'],
            
            # MA
            'ma_5': self.calc_ma(closes, 5),
            'ma_10': self.calc_ma(closes, 10),
            'ma_20': self.calc_ma(closes, 20),
            'ma_60': self.calc_ma(closes, 60) if len(closes) >= 60 else 0,
            
            # 量比
            'vr': self.calc_vol_ratio(kl),
            'vr_3': self.calc_vol_ratio(kl, -3) if len(kl) >= 3 else 1,
            
            # 乖离率
            'bias': self.calc_bias(closes),
            
            # 布林带
            'bollinger': bollinger,
            'boll_position': self.calc_boll_position(closes),
            
            # 动量
            'momentum_5': self.calc_momentum(closes, 5),
            'momentum_10': self.calc_momentum(closes, 10),
            
            # 趋势判断
            'above_ma20': closes[-1] > self.calc_ma(closes, 20),
            'ma5_above_ma20': self.calc_ma(closes, 5) > self.calc_ma(closes, 20),
            
            # 波动率
            'volatility_5': self.calc_momentum(closes, 5) / closes[-6] * 100 if len(closes) >= 6 else 0,
            
            # 新增因子
            'williams_r': self.calc_williams_r(highs[-1], lows[-1], closes[-1]),
            'psy': self.calc_psy(closes),
            
            # 价格/MA比率
            'price_ma5': closes[-1] / self.calc_ma(closes, 5) if self.calc_ma(closes, 5) > 0 else 1,
            'price_ma20': closes[-1] / self.calc_ma(closes, 20) if self.calc_ma(closes, 20) > 0 else 1,
            'price_ma60': closes[-1] / self.calc_ma(closes, 60) if len(closes) >= 60 and self.calc_ma(closes, 60) > 0 else 1,
        }
        
        return factors


class V3ExperienceFactors:
    """V3经验规则因子化"""
    
    @staticmethod
    def oversold_rebound_factor(rsi: float, vr: float, price_change: float) -> float:
        """
        超跌反弹因子 - V3核心规则
        
        Returns:
            float: 评分 (负分=风险, 正分=机会)
        """
        score = 0
        reasons = []
        
        # 量比权重因子
        weight = 1.0
        if vr < 0.6:
            weight = 0.3
        elif vr < 0.8:
            weight = 0.6
        elif vr >= 1.0:
            weight = 1.2
        
        # ===== RSI < 25 超跌区域 =====
        if rsi < 25:
            if vr > 1.2:
                score = 12
                reasons.append("严重超跌反弹")
            elif vr > 0.8:
                score = 6
                reasons.append("超跌可能反弹")
            else:
                score = -5
                reasons.append("超跌缩量谨慎")
        
        # ===== RSI 25-40 低位区域 =====
        elif rsi < 40:
            if vr > 1.3:
                score = 15
                reasons.append("低位放量见底 ⭐")
            elif vr > 1.0:
                score = 10
                reasons.append("低位温和放量")
            elif vr >= 0.8:
                score = 5
                reasons.append("低位整理")
            else:
                score = 2
                reasons.append("低位缩量观望")
        
        # ===== RSI 40-55 中性区域 =====
        elif rsi < 55:
            if 2 <= price_change <= 8:
                score = 8
                reasons.append("涨幅健康")
            elif price_change < 0:
                score = 3
                reasons.append("调整充分")
            else:
                score = 2
        
        # ===== RSI 55-70 高位区域 =====
        elif rsi < 70:
            if vr > 1.5:
                score = -12
                reasons.append("高位放量风险 ⭐规避")
            elif price_change > 10:
                score = -5
                reasons.append("涨幅过大")
            elif 2 <= price_change <= 8:
                score = 3
                reasons.append("健康上涨")
        
        # ===== RSI > 70 极端高位 =====
        else:
            if vr > 1.5:
                score = -18
                reasons.append("高位放量主力出货 ⭐规避")
            else:
                score = -8
                reasons.append("RSI高位")
        
        # 5日超跌反弹加分
        if price_change < -15 and rsi < 45:
            score += 5
            reasons.append("超跌后反弹")
        
        # 涨幅过大扣分
        if price_change > 12:
            score -= 3
            reasons.append("涨幅过大")
        
        return score * weight, reasons


class V45ExperienceFactors:
    """
    V4.5深度因子规则
    基于20年47,267样本回测验证
    
    核心发现:
    - RSI55-65 + 跌幅10-6% → 准确率73.6% ⭐⭐⭐最强
    - 10日跌幅20-15% → 准确率60.6%
    - RSI45-55 + 涨幅>6% → 准确率36.8% ❌规避
    - PSY>85 → 准确率34% ❌必须离场
    """
    
    @staticmethod
    def comprehensive_score(factors: Dict) -> tuple:
        """
        V4.5综合评分
        返回: (评分, 理由列表, 置信度, 方向, 信号列表)
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
        
        # 信号10: 威廉<-80 + RSI>70 → 准确率56.5%
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
        
        # 信号17: RSI<25 + 涨幅2-6% → 准确率59.5%
        if rsi14 < 25 and 2 <= pct < 6:
            score += 12
            reasons.append("RSI<25+反弹进行中 [准确率59.5%]")
            signals.append(('BUY', 12))
        
        # ========== 辅助加分 ==========
        
        if williams_r < -80:
            score += 4
            reasons.append("威廉指标极度超卖")
        
        if kdj_j < 0:
            score += 3
            reasons.append("KDJ_J低位")
        
        if boll_pos < 0.1:
            score += 4
            reasons.append("布林带下轨")
        
        # ========== 规避信号 ==========
        
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
        
        final_score = score - avoid_score
        all_reasons = reasons + avoid_reasons
        
        # 置信度
        confidence = 0.5
        if abs(final_score) >= 20:
            confidence = 0.85
        elif abs(final_score) >= 15:
            confidence = 0.75
        elif abs(final_score) >= 10:
            confidence = 0.65
        elif abs(final_score) >= 5:
            confidence = 0.55
        
        # 方向
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


class MarketEnvironmentFactors:
    """市场环境因子"""
    
    @staticmethod
    def get_market_breadth() -> Dict:
        """获取市场广度数据"""
        try:
            url = 'http://push2.eastmoney.com/api/qt/stock/get?secid=1.000001&fields=f43,f44,f45,f46,f47,f48,f57,f58,f60,f81,f105,f106,f123,f124,f127,f128'
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'http://quote.eastmoney.com/'})
            with urllib.request.urlopen(req, timeout=5) as r:
                data = json.loads(r.read().decode('utf-8', errors='ignore'))
            return {'data': data}
        except:
            return {'data': None}
    
    @staticmethod
    def judge_market_condition(
        index_above_ma20: bool = False,
        ma5_above_ma20: bool = False,
        index_change: float = 0,
        advance_ratio: float = 0.5,
        volume_ratio: float = 1.0
    ) -> float:
        """
        市场环境评分
        """
        score = 0
        
        # 指数技术状态
        if index_above_ma20 and ma5_above_ma20:
            score += 0.3
        elif index_above_ma20:
            score += 0.15
        
        if index_change > 0:
            score += 0.1
        elif index_change < -1:
            score -= 0.1
        
        # 市场广度
        if advance_ratio > 0.6:
            score += 0.25
        elif advance_ratio < 0.4:
            score -= 0.15
        
        # 成交量
        if volume_ratio > 1.2:
            score += 0.2
        elif volume_ratio < 0.8:
            score -= 0.1
        
        return min(max(score, 0), 1)
    
    @staticmethod
    def market_momentum_index() -> Dict:
        """市场动量指数配置"""
        return {
            'index_ma': 0.3,
            'advance_decline': 0.25,
            'volume_trend': 0.2,
            'sector_rotation': 0.15,
            'sentiment': 0.1
        }


class SectorSynergyFactors:
    """板块协同因子"""
    
    @staticmethod
    def sector_momentum(sector_stocks: List[Dict]) -> Dict:
        """板块动量分析"""
        if len(sector_stocks) < 3:
            return {'sector_score': 0, '共振度': 0, '信号': '数据不足'}
        
        signals = []
        bullish_count = 0
        bearish_count = 0
        
        for stock in sector_stocks:
            rsi = stock.get('rsi', 50)
            vr = stock.get('vr', 1)
            
            if rsi < 40 and vr > 1.2:
                signals.append('bullish')
                bullish_count += 1
            elif rsi > 60 and vr > 1.5:
                signals.append('bearish')
                bearish_count += 1
            else:
                signals.append('neutral')
        
        total = len(signals)
        bull_ratio = bullish_count / total
        bear_ratio = bearish_count / total
        
        if bull_ratio > 0.6:
            sector_score = 1.0
            signal = '强势'
        elif bear_ratio > 0.6:
            sector_score = -1.0
            signal = '弱势'
        elif bull_ratio > 0.4:
            sector_score = 0.5
            signal = '偏强'
        elif bear_ratio > 0.4:
            sector_score = -0.5
            signal = '偏弱'
        else:
            sector_score = 0
            signal = '震荡'
        
        return {
            'sector_score': sector_score,
            'bull_ratio': bull_ratio,
            'bear_ratio': bear_ratio,
            '信号': signal,
            '共振度': max(bull_ratio, bear_ratio)
        }


class AdaptiveFactorWeights:
    """动态因子权重"""
    
    def __init__(self):
        self.base_weights = {
            'v3_experience': 0.4,
            'momentum': 0.2,
            'volume': 0.15,
            'market_env': 0.15,
            'sector_sync': 0.1
        }
    
    def adjust_for_market_condition(self, market_score: float) -> Dict:
        """根据市场评分调整因子权重"""
        adjusted = self.base_weights.copy()
        
        if market_score > 0.6:  # 强势市场
            adjusted['v3_experience'] *= 1.3
            adjusted['momentum'] *= 1.2
            adjusted['market_env'] *= 0.8
        elif market_score < 0.4:  # 弱势市场
            adjusted['v3_experience'] *= 0.6
            adjusted['market_env'] *= 1.5
            adjusted['sector_sync'] *= 1.3
        
        # 归一化
        total = sum(adjusted.values())
        for key in adjusted:
            adjusted[key] /= total
        
        return adjusted


class SignalFilter:
    """信号过滤"""
    
    def __init__(self):
        self.min_confidence = {
            'bull_market': 0.5,
            'bear_market': 0.6,
            'neutral': 0.55
        }
    
    def filter_by_confidence(self, signals: List[Dict], confidence_threshold: float) -> List[Dict]:
        """按置信度过滤"""
        return [s for s in signals if s.get('confidence', 0) >= confidence_threshold]
    
    def filter_by_volatility(self, signals: List[Dict], max_volatility: float = 15) -> List[Dict]:
        """按波动率过滤"""
        return [s for s in signals if abs(s.get('change_pct', 0)) <= max_volatility]


class CompositeScorer:
    """综合评分器"""
    
    def __init__(self):
        self.v3 = V3ExperienceFactors()
        self.v45 = V45ExperienceFactors()
    
    def score(self, factors: Dict, use_v45: bool = True) -> Dict:
        """
        综合评分
        
        Args:
            factors: 技术因子字典
            use_v45: 是否使用V4.5规则
        """
        if use_v45:
            score, reasons, confidence, direction, signals = self.v45.comprehensive_score(factors)
        else:
            v3_score, v3_reasons = self.v3.oversold_rebound_factor(
                factors.get('rsi_14', 50),
                factors.get('vr', 1),
                factors.get('change_pct', 0)
            )
            score = v3_score
            reasons = v3_reasons
            confidence = 0.5
            direction = '推荐' if score > 5 else '规避' if score < -5 else '观望'
            signals = []
        
        return {
            'score': score,
            'reasons': reasons,
            'confidence': confidence,
            'direction': direction,
            'signals': signals
        }