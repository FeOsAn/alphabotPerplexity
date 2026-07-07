    Fetching data (cached after first run)...
    
======================================================================
PART 1a — RETURN-ENGINE EQUITY CURVE (vol-targeted basket, 2015-2026)
======================================================================
    Full period: CAGR 17.7% | Sharpe 1.31 | Sortino 1.72 | MaxDD -15.9% | Calmar 1.11
    [saved] deep_analysis_equity_curve.csv (145 months)
    
Annual return — engine vs SPY:
      2014: engine   +7.5%   SPY   +8.2%   lag 
      2015: engine   +9.7%   SPY   +1.2%   BEAT
      2016: engine  +15.3%   SPY  +12.0%   BEAT
      2017: engine  +36.8%   SPY  +21.7%   BEAT
      2018: engine   +4.4%   SPY   -4.6%   BEAT
      2019: engine  +26.5%   SPY  +31.2%   lag 
      2020: engine  +20.5%   SPY  +18.3%   BEAT
      2021: engine  +30.1%   SPY  +28.7%   BEAT
      2022: engine   -9.7%   SPY  -18.2%   BEAT
      2023: engine  +32.7%   SPY  +26.2%   BEAT
      2024: engine  +28.1%   SPY  +24.9%   BEAT
      2025: engine  +15.0%   SPY  +17.7%   lag 
      2026: engine   +4.9%   SPY   +9.2%   lag 
    
SPY monthly-return correlation: 0.90
    
Worst 8 drawdown episodes (peak → trough, depth, recovery days):
      2021-11-09 → 2022-09-30   -15.9%  recovered 2023-06-01 (244d)
      2020-02-20 → 2020-03-23   -15.0%  recovered 2020-08-12 (142d)
      2018-10-02 → 2018-12-24   -14.7%  recovered 2019-04-23 (120d)
      2025-02-20 → 2025-04-08   -14.2%  recovered 2025-07-25 (108d)
      2015-12-07 → 2016-02-11   -11.6%  recovered 2016-04-18 (67d)
      2018-01-29 → 2018-04-02   -10.7%  recovered 2018-07-17 (106d)
      2020-09-03 → 2020-10-28    -9.4%  recovered 2020-12-04 (37d)
      2015-07-21 → 2015-08-25    -9.2%  recovered 2015-11-02 (69d)
    
======================================================================
PART 1b — STRATEGY ATTRIBUTION (backtest-reconstructed, 2015-2026)
======================================================================
    (v100 live config: gap_scanner + cs_momentum DISABLED; mean_reversion now RSI(2))
    strategy          status    trades  win%  exp/tr    PF mSharpe
    multi_tf_rsi      active      3587   43%   0.36%  1.23    0.97
    trend_pullback    active      3764   43%   0.30%  1.20    0.62
    breakout          active      2470   43%   0.34%  1.24    0.10
    52wh_vol          active      1794   42%   0.30%  1.22   -0.10
    mean_reversion    active      4348   60%   0.08%  1.09    0.71
    gap_scanner       DISABLED    2346   40%   0.09%  1.05    0.96
    momentum          active       554   48%   0.33%  1.21    0.05
    quality_momentum  active       393   44%   0.04%  1.03    0.11
    cs_momentum       DISABLED     527   42%  -0.15%  0.92   -0.07
    [saved] deep_analysis_strategy_attribution.json
    
======================================================================
PART 1c — REGIME BREAKDOWN (SPY/200DMA + VIX proxy, 2015-2026)
======================================================================
      BULL        1698 days (55.9%)  engine cum +1550.4%  SPY cum +1116.8%
      CHOP         326 days (10.7%)  engine cum   -22.2%  SPY cum   -19.2%
      TRANSITION   310 days (10.2%)  engine cum   -18.2%  SPY cum   -11.6%
      BEAR         503 days (16.6%)  engine cum   -38.5%  SPY cum   -50.3%
    
======================================================================
PART 2 — PARAMETER GRIDS
======================================================================
    
2a. Exposure cap (vol-targeted engine, full period):
      cap 0.6: CAGR  13.2%  Sharpe 1.31  MaxDD -12.0%
      cap 0.7: CAGR  15.4%  Sharpe 1.31  MaxDD -14.0%
      cap 0.8: CAGR  17.7%  Sharpe 1.31  MaxDD -15.9%
      cap 0.9: CAGR  19.9%  Sharpe 1.31  MaxDD -17.9%
      cap 1.0: CAGR  22.2%  Sharpe 1.31  MaxDD -19.8%
    
2c. Stop x TP(ATR) grid — momentum entry (multi_tf_rsi signal), Sharpe:
    [saved] deep_analysis_backtest_grid.json
    
======================================================================
PART 3 — NEW STRATEGY RESEARCH (price-based; 3d/3f need fundamentals=blocked)
======================================================================
    3b vol-adj momentum: N=782 win 44% exp +0.18% PF 1.11
    3c mean-rev RSI(2)<10 >MA200: N=3513 win 42% exp +0.52% PF 1.36
    3e Donchian best N=40/M=20: N=2102 win 39% exp +2.30% PF 1.98
    [saved] deep_analysis_new_strategies.json
    
======================================================================
PART 4 — TAIL RISK, CONCENTRATION, CORRELATION
======================================================================
    
4a. Stress windows (engine vs SPY, cumulative return + maxDD):
      COVID 2020    engine  -14.3% (DD -15.0%)   SPY  -33.4%
      2022 bear     engine  -14.5% (DD -15.8%)   SPY  -23.8%
      2018 Q4       engine  -13.8% (DD -14.7%)   SPY  -18.9%
      Volmageddon   engine   -6.3% (DD  -7.8%)   SPY   -5.9%
    
4b/4c. Current book concentration + 6mo correlation:
      Current-weight portfolio vol: 11.9%/yr | equal-weight: 13.2%/yr
      Top-3 concentration: V 17%, ABBV 17%, MS 16% = 50% of gross
      Financials ['JPM', 'MS', 'V'] avg pairwise corr: 0.36
    
  6-month return correlation (rounded):
            PANW   DDOG    JPM   AAPL    LLY  GOOGL      V   ABBV    LOW     MS    LMT    XOM    QQQ     HD     MU
      PANW    1.00   0.61   0.03   0.04   0.09   0.15   0.12   0.02  -0.14   0.20  -0.07  -0.17   0.34  -0.11   0.07
      DDOG    0.61   1.00  -0.09   0.13   0.02   0.05   0.17  -0.05  -0.13   0.12  -0.12  -0.17   0.25  -0.08   0.09
      JPM     0.03  -0.09   1.00   0.19   0.10   0.16   0.36   0.05   0.24   0.59   0.16  -0.10   0.22   0.27  -0.01
      AAPL    0.04   0.13   0.19   1.00   0.19   0.22   0.15  -0.06   0.14   0.34  -0.17  -0.16   0.35   0.17   0.04
      LLY     0.09   0.02   0.10   0.19   1.00   0.26  -0.02   0.34   0.17  -0.07   0.03  -0.12   0.06   0.25  -0.02
      GOOGL   0.15   0.05   0.16   0.22   0.26   1.00   0.07   0.06   0.28   0.31   0.06  -0.31   0.53   0.27   0.24
      V       0.12   0.17   0.36   0.15  -0.02   0.07   1.00   0.15   0.07   0.14  -0.01  -0.05   0.08   0.16  -0.11
      ABBV    0.02  -0.05   0.05  -0.06   0.34   0.06   0.15   1.00   0.11  -0.09   0.10  -0.03  -0.07   0.10  -0.08
      LOW    -0.14  -0.13   0.24   0.14   0.17   0.28   0.07   0.11   1.00   0.14   0.16  -0.18   0.17   0.90  -0.03
      MS      0.20   0.12   0.59   0.34  -0.07   0.31   0.14  -0.09   0.14   1.00   0.08  -0.23   0.53   0.14   0.28
      LMT    -0.07  -0.12   0.16  -0.17   0.03   0.06  -0.01   0.10   0.16   0.08   1.00   0.13  -0.04   0.10   0.00
      XOM    -0.17  -0.17  -0.10  -0.16  -0.12  -0.31  -0.05  -0.03  -0.18  -0.23   0.13   1.00  -0.35  -0.25  -0.23
      QQQ     0.34   0.25   0.22   0.35   0.06   0.53   0.08  -0.07   0.17   0.53  -0.04  -0.35   1.00   0.23   0.68
      HD     -0.11  -0.08   0.27   0.17   0.25   0.27   0.16   0.10   0.90   0.14   0.10  -0.25   0.23   1.00   0.01
      MU      0.07   0.09  -0.01   0.04  -0.02   0.24  -0.11  -0.08  -0.03   0.28   0.00  -0.23   0.68   0.01   1.00