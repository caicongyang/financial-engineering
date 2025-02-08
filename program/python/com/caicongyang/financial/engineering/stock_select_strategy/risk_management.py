def calculate_position_size(strength_score, market_status):
    """根据市场状态和概念强度计算仓位"""
    base_size = {
        'bull': 0.2,
        'neutral': 0.1,
        'bear': 0.05
    }[market_status]
    
    risk_adjustment = 1 - (strength_score / 100)  # 强度得分越高仓位越大
    return min(base_size * risk_adjustment, 0.3)  # 单概念最大仓位30% 