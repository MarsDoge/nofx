package trader

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"nofx/config"
	"nofx/decision"
	"nofx/logger"
	"nofx/market"
	"nofx/mcp"
	"nofx/pool"
	"regexp"
	"strings"
	"sync"
	"time"
)

// AutoTraderConfig 自动交易配置（简化版 - AI全权决策）
type AutoTraderConfig struct {
	// Trader标识
	ID      string // Trader唯一标识（用于日志目录等）
	Name    string // Trader显示名称
	AIModel string // AI模型: "qwen" 或 "deepseek"

	// 交易平台选择
	Exchange string // "binance", "hyperliquid" 或 "aster"

	// 币安API配置
	BinanceAPIKey    string
	BinanceSecretKey string

	// Hyperliquid配置
	HyperliquidPrivateKey string
	HyperliquidWalletAddr string
	HyperliquidTestnet    bool

	// Aster配置
	AsterUser       string // Aster主钱包地址
	AsterSigner     string // Aster API钱包地址
	AsterPrivateKey string // Aster API钱包私钥

	CoinPoolAPIURL string
	OITopAPIURL    string

	// AI配置
	UseQwen     bool
	DeepSeekKey string
	QwenKey     string

	// 自定义AI API配置
	CustomAPIURL    string
	CustomAPIKey    string
	CustomModelName string

	// 扫描配置
	ScanInterval time.Duration // 扫描间隔（建议3分钟）

	// 账户配置
	InitialBalance float64 // 初始金额（用于计算盈亏，需手动设置）

	// 杠杆配置
	BTCETHLeverage  int // BTC和ETH的杠杆倍数
	AltcoinLeverage int // 山寨币的杠杆倍数

	// 手续费率配置
	TakerFeeRate float64 // Taker fee rate (default 0.0004)
	MakerFeeRate float64 // Maker fee rate (default 0.0002)

	// 风险控制（仅作为提示，AI可自主决定）
	MaxDailyLoss    float64       // 最大日亏损百分比（提示）
	MaxDrawdown     float64       // 最大回撤百分比（提示）
	StopTradingTime time.Duration // 触发风控后暂停时长

	// 仓位模式
	IsCrossMargin bool // true=全仓模式, false=逐仓模式

	// 币种配置
	DefaultCoins []string // 默认币种列表（从数据库获取）
	TradingCoins []string // 实际交易币种列表

	// 币种池信号源配置
	UseCoinPool bool // 是否使用 AI500 Coin Pool 信号源
	UseOITop    bool // 是否使用 OI Top 增长信号源

	// 系统提示词模板
	SystemPromptTemplate string // 系统提示词模板名称（如 "default", "aggressive"）

	// 订单策略配置
	OrderStrategy       string  // Order strategy: "market_only", "conservative_hybrid", "limit_only"
	LimitPriceOffset    float64 // Limit order price offset percentage (e.g., -0.03 for -0.03%)
	LimitTimeoutSeconds int     // Timeout in seconds before converting to market order

	// K线时间线配置
	Timeframes []string // K线时间线选择，例如: ["1m", "15m", "1h", "4h"]

	// Telegram 通知配置
	EnableTelegramUpdates bool   // 是否启用 Telegram 推送
	TelegramBotToken      string // Telegram Bot Token
	TelegramChatID        int64  // Telegram Chat ID (支持群组，通常为负数)
}

// AutoTrader 自动交易器
type AutoTrader struct {
	id                    string // Trader唯一标识
	name                  string // Trader显示名称
	aiModel               string // AI模型名称
	exchange              string // 交易平台名称
	config                AutoTraderConfig
	trader                Trader // 使用Trader接口（支持多平台）
	mcpClient             mcp.AIClient
	decisionLogger        logger.IDecisionLogger // 决策日志记录器
	initialBalance        float64
	dailyPnL              float64
	dailyPnLBase          float64
	needsDailyBaseline    bool
	customPrompt          string   // 自定义交易策略prompt
	overrideBasePrompt    bool     // 是否覆盖基础prompt
	systemPromptTemplate  string   // 系统提示词模板名称
	timeframes            []string // K线时间线配置
	defaultCoins          []string // 默认币种列表（从数据库获取）
	tradingCoins          []string // 实际交易币种列表
	useCoinPool           bool     // 是否使用 AI500 Coin Pool 信号源
	useOITop              bool     // 是否使用 OI Top 增长信号源
	coinPoolAPIURL        string
	oiTopAPIURL           string
	lastResetTime         time.Time
	stopUntil             time.Time
	isRunning             bool
	startTime             time.Time                        // 系统启动时间
	callCount             int                              // AI调用次数
	positionFirstSeenTime map[string]int64                 // 持仓首次出现时间 (symbol_side -> timestamp毫秒)
	lastPositions         map[string]decision.PositionInfo // 上一次周期的持仓快照 (用于检测被动平仓)
	positionStopLoss      map[string]float64               // 持仓止损价格 (symbol_side -> stop_loss_price)
	positionTakeProfit    map[string]float64               // 持仓止盈价格 (symbol_side -> take_profit_price)
	stopMonitorCh         chan struct{}                    // 用于停止监控goroutine
	monitorWg             sync.WaitGroup                   // 用于等待监控goroutine结束
	peakPnLCache          map[string]float64               // 最高收益缓存 (symbol -> 峰值盈亏百分比)
	peakPnLCacheMutex     sync.RWMutex                     // 缓存读写锁
	peakEquity            float64                          // 账户峰值净值，用于回撤计算
	lastBalanceSyncTime   time.Time                        // 上次余额同步时间
	database              interface{}                      // 数据库引用（用于自动更新余额）
	userID                string                           // 用户ID
	telegramSender        *logger.TelegramSender           // Telegram 推送发送器
}

// NewAutoTrader 创建自动交易器
func NewAutoTrader(config AutoTraderConfig, database interface{}, userID string) (*AutoTrader, error) {
	// 设置默认值
	if config.ID == "" {
		config.ID = "default_trader"
	}
	if config.Name == "" {
		config.Name = "Default Trader"
	}
	if config.AIModel == "" {
		if config.UseQwen {
			config.AIModel = "qwen"
		} else {
			config.AIModel = "deepseek"
		}
	}

	mcpClient := mcp.New()

	// 初始化AI
	if config.AIModel == "custom" {
		// 使用自定义API
		mcpClient.SetAPIKey(config.CustomAPIKey, config.CustomAPIURL, config.CustomModelName)
		log.Printf("🤖 [%s] 使用自定义AI API: %s (模型: %s)", config.Name, config.CustomAPIURL, config.CustomModelName)
	} else if config.UseQwen || config.AIModel == "qwen" {
		// 使用Qwen (支持自定义URL和Model)
		mcpClient = mcp.NewQwenClient()
		mcpClient.SetAPIKey(config.QwenKey, config.CustomAPIURL, config.CustomModelName)
		if config.CustomAPIURL != "" || config.CustomModelName != "" {
			log.Printf("🤖 [%s] 使用阿里云Qwen AI (自定义URL: %s, 模型: %s)", config.Name, config.CustomAPIURL, config.CustomModelName)
		} else {
			log.Printf("🤖 [%s] 使用阿里云Qwen AI", config.Name)
		}
	} else {
		// 默认使用DeepSeek (支持自定义URL和Model)
		mcpClient = mcp.NewDeepSeekClient()
		mcpClient.SetAPIKey(config.DeepSeekKey, config.CustomAPIURL, config.CustomModelName)
		if config.CustomAPIURL != "" || config.CustomModelName != "" {
			log.Printf("🤖 [%s] 使用DeepSeek AI (自定义URL: %s, 模型: %s)", config.Name, config.CustomAPIURL, config.CustomModelName)
		} else {
			log.Printf("🤖 [%s] 使用DeepSeek AI", config.Name)
		}
	}

	// 设置默认交易平台
	if config.Exchange == "" {
		config.Exchange = "binance"
	}

	// 根据配置创建对应的交易器
	var trader Trader
	var err error

	// 记录仓位模式（通用）
	marginModeStr := "全仓"
	if !config.IsCrossMargin {
		marginModeStr = "逐仓"
	}
	log.Printf("📊 [%s] 仓位模式: %s", config.Name, marginModeStr)

	switch config.Exchange {
	case "binance":
		log.Printf("🏦 [%s] 使用币安合约交易", config.Name)
		trader = NewFuturesTrader(
			config.BinanceAPIKey,
			config.BinanceSecretKey,
			userID,
			config.OrderStrategy,
			config.LimitPriceOffset,
			config.LimitTimeoutSeconds,
		)
	case "hyperliquid":
		log.Printf("🏦 [%s] 使用Hyperliquid交易", config.Name)
		trader, err = NewHyperliquidTrader(config.HyperliquidPrivateKey, config.HyperliquidWalletAddr, config.HyperliquidTestnet)
		if err != nil {
			return nil, fmt.Errorf("初始化Hyperliquid交易器失败: %w", err)
		}
	case "aster":
		log.Printf("🏦 [%s] 使用Aster交易", config.Name)
		trader, err = NewAsterTrader(config.AsterUser, config.AsterSigner, config.AsterPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("初始化Aster交易器失败: %w", err)
		}
	default:
		return nil, fmt.Errorf("不支持的交易平台: %s", config.Exchange)
	}

	config.ScanInterval = enforceScanInterval(config.ScanInterval, config.Name)
	config.Timeframes = ensureTimeframes(config.Timeframes, config.Name)

	// 验证初始金额配置
	if config.InitialBalance <= 0 {
		return nil, fmt.Errorf("初始金额必须大于0，请在配置中设置InitialBalance")
	}

	// 初始化决策日志记录器（使用trader ID创建独立目录）
	logDir := fmt.Sprintf("decision_logs/%s", config.ID)
	decisionLogger := logger.NewDecisionLogger(logDir)

	// 设置默认系统提示词模板
	systemPromptTemplate := config.SystemPromptTemplate
	if systemPromptTemplate == "" {
		// feature/partial-close-dynamic-tpsl 分支默认使用 adaptive（支持动态止盈止损）
		systemPromptTemplate = "adaptive"
	}

	var telegramSender *logger.TelegramSender
	if config.EnableTelegramUpdates && config.TelegramBotToken != "" && config.TelegramChatID != 0 {
		sender, err := logger.NewTelegramSender(config.TelegramBotToken, config.TelegramChatID)
		if err != nil {
			log.Printf("⚠️ [%s] 初始化 Telegram 通知失败: %v", config.Name, err)
		} else {
			telegramSender = sender
			log.Printf("📨 [%s] 已启用 Telegram 决策推送", config.Name)
		}
	}

	return &AutoTrader{
		id:                    config.ID,
		name:                  config.Name,
		aiModel:               config.AIModel,
		exchange:              config.Exchange,
		config:                config,
		trader:                trader,
		mcpClient:             mcpClient,
		decisionLogger:        decisionLogger,
		initialBalance:        config.InitialBalance,
		systemPromptTemplate:  systemPromptTemplate,
		timeframes:            config.Timeframes, // K线时间线配置
		defaultCoins:          config.DefaultCoins,
		tradingCoins:          config.TradingCoins,
		useCoinPool:           config.UseCoinPool,
		useOITop:              config.UseOITop,
		lastResetTime:         time.Now(),
		dailyPnLBase:          config.InitialBalance,
		needsDailyBaseline:    true,
		peakEquity:            config.InitialBalance,
		startTime:             time.Now(),
		callCount:             0,
		isRunning:             false,
		positionFirstSeenTime: make(map[string]int64),
		lastPositions:         make(map[string]decision.PositionInfo),
		positionStopLoss:      make(map[string]float64),
		positionTakeProfit:    make(map[string]float64),
		stopMonitorCh:         make(chan struct{}),
		monitorWg:             sync.WaitGroup{},
		peakPnLCache:          make(map[string]float64),
		peakPnLCacheMutex:     sync.RWMutex{},
		lastBalanceSyncTime:   time.Now(), // 初始化为当前时间
		database:              database,
		userID:                userID,
		coinPoolAPIURL:        strings.TrimSpace(config.CoinPoolAPIURL),
		oiTopAPIURL:           strings.TrimSpace(config.OITopAPIURL),
		telegramSender:        telegramSender,
	}, nil
}

// Run 运行自动交易主循环
func (at *AutoTrader) Run() error {
	at.isRunning = true
	at.stopMonitorCh = make(chan struct{})
	at.startTime = time.Now()

	log.Println("🚀 AI驱动自动交易系统启动")
	log.Printf("💰 初始余额: %.2f USDT", at.initialBalance)
	log.Printf("⚙️  扫描间隔: %v", at.config.ScanInterval)
	log.Println("🤖 AI将全权决定杠杆、仓位大小、止损止盈等参数")
	at.monitorWg.Add(1)
	defer at.monitorWg.Done()

	// 启动回撤监控
	at.startDrawdownMonitor()

	ticker := time.NewTicker(at.config.ScanInterval)
	defer ticker.Stop()

	// 首次立即执行
	if err := at.runCycle(); err != nil {
		log.Printf("❌ 执行失败: %v", err)
	}

	for at.isRunning {
		select {
		case <-ticker.C:
			if err := at.runCycle(); err != nil {
				log.Printf("❌ 执行失败: %v", err)
			}
		case <-at.stopMonitorCh:
			log.Printf("[%s] ⏹ 收到停止信号，退出自动交易主循环", at.name)
			return nil
		}
	}

	return nil
}

// Stop 停止自动交易
func (at *AutoTrader) Stop() {
	if !at.isRunning {
		return
	}
	at.isRunning = false
	close(at.stopMonitorCh) // 通知监控goroutine停止
	at.monitorWg.Wait()     // 等待监控goroutine结束
	log.Println("⏹ 自动交易系统停止")
}

// Shutdown 释放自动交易器资源（在移除交易员或程序退出时调用）
func (at *AutoTrader) Shutdown() {
	if at.telegramSender != nil {
		at.telegramSender.Stop()
		at.telegramSender = nil
	}
}

// runCycle 运行一个交易周期（使用AI全权决策）
func (at *AutoTrader) runCycle() error {
	at.callCount++
	cycleStart := time.Now()

	log.Print("\n" + strings.Repeat("=", 70) + "\n")
	log.Printf("⏰ %s - AI决策周期 #%d", cycleStart.Format("2006-01-02 15:04:05"), at.callCount)
	log.Println(strings.Repeat("=", 70))

	// 创建决策记录
	record := &logger.DecisionRecord{
		Exchange:     at.config.Exchange, // 记录交易所类型，用于计算手续费
		ExecutionLog: []string{},
		Success:      true,
	}

	// 1. 检查是否需要停止交易
	if time.Now().Before(at.stopUntil) {
		remaining := at.stopUntil.Sub(time.Now())
		log.Printf("⏸ 风险控制：暂停交易中，剩余 %.0f 分钟", remaining.Minutes())
		record.Success = false
		record.ErrorMessage = fmt.Sprintf("风险控制暂停中，剩余 %.0f 分钟", remaining.Minutes())
		if err := at.saveAndNotifyDecision(record, nil, cycleStart, time.Since(cycleStart)); err != nil {
			log.Printf("⚠ 保存决策记录失败: %v", err)
		}
		return nil
	}

	// 2. 重置日盈亏基线（每天一次）
	at.maybeResetDailyMetrics()

	// 4. 收集交易上下文
	ctx, err := at.buildTradingContext()
	if err != nil {
		record.Success = false
		record.ErrorMessage = fmt.Sprintf("构建交易上下文失败: %v", err)
		if saveErr := at.saveAndNotifyDecision(record, nil, cycleStart, time.Since(cycleStart)); saveErr != nil {
			log.Printf("⚠ 保存决策记录失败: %v", saveErr)
		}
		return fmt.Errorf("构建交易上下文失败: %w", err)
	}

	// 保存账户状态快照
	record.AccountState = logger.AccountSnapshot{
		TotalBalance:          ctx.Account.TotalEquity - ctx.Account.UnrealizedPnL,
		AvailableBalance:      ctx.Account.AvailableBalance,
		TotalUnrealizedProfit: ctx.Account.UnrealizedPnL,
		PositionCount:         ctx.Account.PositionCount,
		MarginUsedPct:         ctx.Account.MarginUsedPct,
		InitialBalance:        at.initialBalance, // 记录当时的初始余额基准
	}

	// 保存持仓快照
	for _, pos := range ctx.Positions {
		record.Positions = append(record.Positions, logger.PositionSnapshot{
			Symbol:           pos.Symbol,
			Side:             pos.Side,
			PositionAmt:      pos.Quantity,
			EntryPrice:       pos.EntryPrice,
			MarkPrice:        pos.MarkPrice,
			UnrealizedProfit: pos.UnrealizedPnL,
			Leverage:         float64(pos.Leverage),
			LiquidationPrice: pos.LiquidationPrice,
		})
	}

	// 更新盈亏指标并执行账户级风控
	if reason, triggered := at.enforceRiskLimits(ctx.Account.TotalEquity); triggered {
		record.Success = false
		record.ErrorMessage = reason
		if err := at.saveAndNotifyDecision(record, nil, cycleStart, time.Since(cycleStart)); err != nil {
			log.Printf("⚠ 保存决策记录失败: %v", err)
		}
		log.Printf("⛔ 风险控制触发，暂停交易：%s | 恢复时间: %s", reason, at.stopUntil.Format(time.RFC3339))
		return nil
	}

	// 检测被动平仓（止损/止盈/强平/手动）
	closedPositions := at.detectClosedPositions(ctx.Positions)
	if len(closedPositions) > 0 {
		autoCloseActions := at.generateAutoCloseActions(closedPositions)
		record.Decisions = append(record.Decisions, autoCloseActions...)
		log.Printf("🔔 检测到 %d 个被动平仓", len(closedPositions))
		for i, closed := range closedPositions {
			action := autoCloseActions[i]
			pnl := closed.Quantity * (closed.MarkPrice - closed.EntryPrice)
			if closed.Side == "short" {
				pnl = -pnl
			}
			pnlPct := pnl / (closed.EntryPrice * closed.Quantity) * 100 * float64(closed.Leverage)

			// 平仓原因中文映射
			reasonMap := map[string]string{
				"stop_loss":   "止损",
				"take_profit": "止盈",
				"liquidation": "强平",
				"unknown":     "未知",
			}
			reasonCN := reasonMap[action.Error]
			if reasonCN == "" {
				reasonCN = action.Error
			}

			log.Printf("   └─ %s %s | 开仓: %.4f → 平仓: %.4f | 盈亏: %+.2f%% | 原因: %s",
				closed.Symbol,
				closed.Side,
				closed.EntryPrice,
				action.Price, // 使用推断的平仓价格
				pnlPct,
				reasonCN)
		}
	}

	log.Print(strings.Repeat("=", 70))
	for _, coin := range ctx.CandidateCoins {
		record.CandidateCoins = append(record.CandidateCoins, coin.Symbol)
	}

	log.Printf("📊 账户净值: %.2f USDT | 可用: %.2f USDT | 持仓: %d",
		ctx.Account.TotalEquity, ctx.Account.AvailableBalance, ctx.Account.PositionCount)

	// 5. 调用AI获取完整决策
	log.Printf("🤖 正在请求AI分析并决策... [模板: %s]", at.systemPromptTemplate)
	decision, err := decision.GetFullDecisionWithCustomPrompt(ctx, at.mcpClient, at.customPrompt, at.overrideBasePrompt, at.systemPromptTemplate)

	if decision != nil && decision.AIRequestDurationMs > 0 {
		record.AIRequestDurationMs = decision.AIRequestDurationMs
		log.Printf("⏱️ AI调用耗时: %.2f 秒", float64(record.AIRequestDurationMs)/1000)
		record.ExecutionLog = append(record.ExecutionLog,
			fmt.Sprintf("AI调用耗时: %d ms", record.AIRequestDurationMs))
	}

	// 即使有错误，也保存思维链、决策和输入prompt（用于debug）
	if decision != nil {
		record.SystemPrompt = decision.SystemPrompt // 保存系统提示词
		record.InputPrompt = decision.UserPrompt
		record.CoTTrace = decision.CoTTrace
		if len(decision.Decisions) > 0 {
			decisionJSON, _ := json.MarshalIndent(decision.Decisions, "", "  ")
			record.DecisionJSON = string(decisionJSON)
		}
	}

	if err != nil {
		record.Success = false
		record.ErrorMessage = fmt.Sprintf("获取AI决策失败: %v", err)

		// 打印系统提示词和AI思维链（即使有错误，也要输出以便调试）
		if decision != nil {
			log.Print("\n" + strings.Repeat("=", 70) + "\n")
			log.Printf("📋 系统提示词 [模板: %s] (错误情况)", at.systemPromptTemplate)
			log.Println(strings.Repeat("=", 70))
			log.Println(decision.SystemPrompt)
			log.Println(strings.Repeat("=", 70))

			if decision.CoTTrace != "" {
				log.Print("\n" + strings.Repeat("-", 70) + "\n")
				log.Println("💭 AI思维链分析（错误情况）:")
				log.Println(strings.Repeat("-", 70))
				log.Println(decision.CoTTrace)
				log.Println(strings.Repeat("-", 70))
			}
		}

		if saveErr := at.saveAndNotifyDecision(record, decision, cycleStart, time.Since(cycleStart)); saveErr != nil {
			log.Printf("⚠ 保存决策记录失败: %v", saveErr)
		}
		return fmt.Errorf("获取AI决策失败: %w", err)
	}

	// // 5. 打印系统提示词
	// log.Printf("\n" + strings.Repeat("=", 70))
	// log.Printf("📋 系统提示词 [模板: %s]", at.systemPromptTemplate)
	// log.Println(strings.Repeat("=", 70))
	// log.Println(decision.SystemPrompt)
	// log.Printf(strings.Repeat("=", 70) + "\n")

	// 6. 打印AI思维链
	// log.Printf("\n" + strings.Repeat("-", 70))
	// log.Println("💭 AI思维链分析:")
	// log.Println(strings.Repeat("-", 70))
	// log.Println(decision.CoTTrace)
	// log.Printf(strings.Repeat("-", 70) + "\n")

	// 7. 打印AI决策
	// log.Printf("📋 AI决策列表 (%d 个):\n", len(decision.Decisions))
	// for i, d := range decision.Decisions {
	//     log.Printf("  [%d] %s: %s - %s", i+1, d.Symbol, d.Action, d.Reasoning)
	//     if d.Action == "open_long" || d.Action == "open_short" {
	//        log.Printf("      杠杆: %dx | 仓位: %.2f USDT | 止损: %.4f | 止盈: %.4f",
	//           d.Leverage, d.PositionSizeUSD, d.StopLoss, d.TakeProfit)
	//     }
	// }
	log.Println()
	log.Print(strings.Repeat("-", 70))
	// 8. 对决策排序：确保先平仓后开仓（防止仓位叠加超限）
	log.Print(strings.Repeat("-", 70))

	// 8. 对决策排序：确保先平仓后开仓（防止仓位叠加超限）
	sortedDecisions := sortDecisionsByPriority(decision.Decisions)

	log.Println("🔄 执行顺序（已优化）: 先平仓→后开仓")
	for i, d := range sortedDecisions {
		log.Printf("  [%d] %s %s", i+1, d.Symbol, d.Action)
	}
	log.Println()

	// 执行决策并记录结果
	for _, d := range sortedDecisions {
		decisionToExecute := d
		actionRecord := logger.DecisionAction{
			Action:    decisionToExecute.Action,
			Symbol:    decisionToExecute.Symbol,
			Quantity:  0,
			Leverage:  decisionToExecute.Leverage,
			Price:     0,
			Timestamp: time.Now(),
			Success:   false,
		}

		if forced, msg := at.enforceReasoningOrWait(&decisionToExecute); forced {
			actionRecord.Action = decisionToExecute.Action
			actionRecord.Success = true
			record.ExecutionLog = append(record.ExecutionLog, msg)
			record.Decisions = append(record.Decisions, actionRecord)
			continue
		}

		if err := at.executeDecisionWithRecord(&decisionToExecute, &actionRecord); err != nil {
			log.Printf("❌ 执行决策失败 (%s %s): %v", decisionToExecute.Symbol, decisionToExecute.Action, err)
			actionRecord.Error = err.Error()
			record.ExecutionLog = append(record.ExecutionLog,
				fmt.Sprintf("❌ %s %s 失败: %v", decisionToExecute.Symbol, decisionToExecute.Action, err))
		} else {
			actionRecord.Success = true
			record.ExecutionLog = append(record.ExecutionLog,
				fmt.Sprintf("✓ %s %s 成功", decisionToExecute.Symbol, decisionToExecute.Action))
			// 成功执行后短暂延迟
			time.Sleep(1 * time.Second)
		}

		record.Decisions = append(record.Decisions, actionRecord)
	}

	// 9. 更新持仓快照（用于下一周期检测被动平仓）
	at.updatePositionSnapshot(ctx.Positions)

	// 10. 保存决策记录
	if err := at.saveAndNotifyDecision(record, decision, cycleStart, time.Since(cycleStart)); err != nil {
		log.Printf("⚠ 保存决策记录失败: %v", err)
	}

	return nil
}

// saveAndNotifyDecision 保存决策记录并尝试推送 Telegram 更新
func (at *AutoTrader) saveAndNotifyDecision(record *logger.DecisionRecord, aiDecision *decision.FullDecision, cycleStart time.Time, cycleDuration time.Duration) error {
	if record == nil {
		return nil
	}

	err := at.decisionLogger.LogDecision(record)

	if at.telegramSender != nil {
		message := at.buildTelegramMessage(record, aiDecision, cycleStart, cycleDuration)
		if message != "" {
			at.telegramSender.SendAsync(message)
		}
	}

	return err
}

// buildTelegramMessage 构建推送到 Telegram 的 Markdown 文本
func (at *AutoTrader) buildTelegramMessage(record *logger.DecisionRecord, aiDecision *decision.FullDecision, cycleStart time.Time, cycleDuration time.Duration) string {
	if record == nil {
		return ""
	}

	var builder strings.Builder

	cycleNumber := record.CycleNumber
	if cycleNumber <= 0 {
		cycleNumber = at.callCount
	}

	builder.WriteString(fmt.Sprintf("🤖 *交易AI决策更新* (周期 #%d)\n", cycleNumber))
	builder.WriteString(fmt.Sprintf("📛 交易员: %s\n", escapeTelegramMarkdown(at.name)))

	statusEmoji := "🟢"
	statusText := "正常运行"
	if !record.Success {
		statusEmoji = "🟠"
		statusText = "异常"
	}
	builder.WriteString(fmt.Sprintf("状态: %s %s\n", statusText, statusEmoji))
	builder.WriteString(fmt.Sprintf("🕒 周期开始: `%s`\n", cycleStart.Format("2006-01-02 15:04:05")))
	builder.WriteString(fmt.Sprintf("⏱️ 耗时: `%s`\n", formatDuration(cycleDuration)))

	if record.AIRequestDurationMs > 0 {
		builder.WriteString(fmt.Sprintf("🤖 AI耗时: `%s`\n", formatDuration(time.Duration(record.AIRequestDurationMs)*time.Millisecond)))
	}

	if hasAccountSnapshot(record.AccountState) {
		equity := record.AccountState.TotalBalance + record.AccountState.TotalUnrealizedProfit
		builder.WriteString(fmt.Sprintf("💰 权益: `%.2f` USDT | 可用: `%.2f` USDT\n",
			equity, record.AccountState.AvailableBalance))
		builder.WriteString(fmt.Sprintf("📉 未实现盈亏: `%.2f` USDT | 保证金率: `%.1f%%`\n",
			record.AccountState.TotalUnrealizedProfit, record.AccountState.MarginUsedPct))

		if record.AccountState.InitialBalance > 0 {
			roi := (equity - record.AccountState.InitialBalance) / record.AccountState.InitialBalance * 100
			builder.WriteString(fmt.Sprintf("💹 ROI: `%+.2f%%` (起始 `%.2f`)\n",
				roi, record.AccountState.InitialBalance))
		}

		totalSlots := len(at.tradingCoins)
		if totalSlots == 0 {
			totalSlots = len(at.defaultCoins)
		}

		positionSummary := fmt.Sprintf("%d", len(record.Positions))
		if totalSlots > 0 {
			positionSummary = fmt.Sprintf("%d/%d", len(record.Positions), totalSlots)
		}

		builder.WriteString(fmt.Sprintf("📦 持仓: `%s` | 候选币种: `%d`\n", positionSummary, len(record.CandidateCoins)))
	}

	if len(record.CandidateCoins) > 0 {
		builder.WriteString(fmt.Sprintf("📋 候选列表: %s\n",
			escapeTelegramMarkdown(joinAndLimit(record.CandidateCoins, 8))))
	}

	if len(record.Positions) > 0 {
		builder.WriteString("\n📊 *持仓明细:*\n")
		positionLines := formatPositionLines(record.Positions, 3)
		for _, line := range positionLines {
			builder.WriteString("• ")
			builder.WriteString(line)
			builder.WriteString("\n")
		}
		if len(record.Positions) > len(positionLines) {
			builder.WriteString(fmt.Sprintf("…其余 %d 个持仓\n", len(record.Positions)-len(positionLines)))
		}
	}

	var decisionLines []string
	if aiDecision != nil {
		for _, d := range aiDecision.Decisions {
			if line := formatDecisionForTelegram(d); line != "" {
				decisionLines = append(decisionLines, line)
			}
		}
	}

	if len(decisionLines) > 0 {
		builder.WriteString("\n🧠 *AI 决策:*\n")
		for _, line := range decisionLines {
			builder.WriteString("• ")
			builder.WriteString(line)
			builder.WriteString("\n")
		}
	}

	if len(record.ExecutionLog) > 0 {
		builder.WriteString("\n🔁 *执行结果:*\n")
		for _, logLine := range record.ExecutionLog {
			builder.WriteString("• ")
			builder.WriteString(escapeTelegramMarkdown(logLine))
			builder.WriteString("\n")
		}
	}

	if record.ErrorMessage != "" {
		builder.WriteString("\n❌ *错误:* ")
		builder.WriteString(escapeTelegramMarkdown(record.ErrorMessage))
		builder.WriteString("\n")
	}

	message := strings.TrimSpace(builder.String())
	runes := []rune(message)
	if len(runes) > 4000 {
		message = string(runes[:4000]) + "…"
	}

	return message
}

func hasAccountSnapshot(snapshot logger.AccountSnapshot) bool {
	return snapshot.TotalBalance != 0 || snapshot.AvailableBalance != 0 || snapshot.TotalUnrealizedProfit != 0 || snapshot.PositionCount != 0 || snapshot.MarginUsedPct != 0 || snapshot.InitialBalance != 0
}

func formatDecisionForTelegram(d decision.Decision) string {
	actionEmoji, actionText := telegramActionLabel(d.Action)
	if actionEmoji == "" && actionText == "" {
		return ""
	}

	var detailParts []string

	switch d.Action {
	case "open_long", "open_short":
		if d.Leverage > 0 {
			detailParts = append(detailParts, fmt.Sprintf("杠杆 %dx", d.Leverage))
		}
		if d.PositionSizeUSD > 0 {
			detailParts = append(detailParts, fmt.Sprintf("仓位 %.2f USDT", d.PositionSizeUSD))
		}
		if d.StopLoss > 0 {
			detailParts = append(detailParts, fmt.Sprintf("止损 %.4f", d.StopLoss))
		}
		if d.TakeProfit > 0 {
			detailParts = append(detailParts, fmt.Sprintf("止盈 %.4f", d.TakeProfit))
		}
	case "update_stop_loss":
		if d.NewStopLoss > 0 {
			detailParts = append(detailParts, fmt.Sprintf("新止损 %.4f", d.NewStopLoss))
		}
	case "update_take_profit":
		if d.NewTakeProfit > 0 {
			detailParts = append(detailParts, fmt.Sprintf("新止盈 %.4f", d.NewTakeProfit))
		}
	case "partial_close":
		if d.ClosePercentage > 0 {
			detailParts = append(detailParts, fmt.Sprintf("平仓 %.0f%%", d.ClosePercentage))
		}
	}

	if d.RiskUSD > 0 {
		detailParts = append(detailParts, fmt.Sprintf("风险 %.2f USDT", d.RiskUSD))
	}
	if d.Confidence > 0 {
		detailParts = append(detailParts, fmt.Sprintf("信心 %d%%", d.Confidence))
	}

	detail := ""
	if len(detailParts) > 0 {
		detail = " (" + strings.Join(detailParts, " | ") + ")"
	}

	reasoning := strings.TrimSpace(d.Reasoning)
	if reasoning != "" {
		reasoning = truncateReason(reasoning, 220)
		reasoning = " — " + escapeTelegramMarkdown(reasoning)
	}

	prefix := strings.TrimSpace(fmt.Sprintf("%s %s", actionEmoji, actionText))
	if prefix == "" {
		prefix = actionEmoji
	}

	return fmt.Sprintf("%s %s%s%s", escapeTelegramMarkdown(prefix), escapeTelegramMarkdown(d.Symbol), escapeTelegramMarkdown(detail), reasoning)
}

func telegramActionLabel(action string) (string, string) {
	switch action {
	case "open_long":
		return "🚀", "开多"
	case "open_short":
		return "📉", "开空"
	case "close_long":
		return "✅", "平多"
	case "close_short":
		return "✅", "平空"
	case "update_stop_loss":
		return "🛡️", "调整止损"
	case "update_take_profit":
		return "🎯", "调整止盈"
	case "partial_close":
		return "↘️", "部分平仓"
	case "hold":
		return "⏸️", "保持"
	case "wait":
		return "⏳", "等待"
	default:
		return "ℹ️", action
	}
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return "0ms"
	}

	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}

	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}

	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60
	if minutes < 60 {
		if seconds == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}

	hours := minutes / 60
	minutes = minutes % 60
	if minutes == 0 && seconds == 0 {
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dh%dm%ds", hours, minutes, seconds)
}

func joinAndLimit(items []string, limit int) string {
	if limit <= 0 || len(items) <= limit {
		return strings.Join(items, ", ")
	}

	truncated := strings.Join(items[:limit], ", ") + "…"
	return truncated
}

func formatPositionLines(positions []logger.PositionSnapshot, limit int) []string {
	if limit <= 0 {
		limit = len(positions)
	}

	lines := make([]string, 0, limit)
	for i, pos := range positions {
		if i >= limit {
			break
		}
		lines = append(lines, formatPositionLine(pos))
	}
	return lines
}

func formatPositionLine(pos logger.PositionSnapshot) string {
	symbol := escapeTelegramMarkdown(pos.Symbol)
	line := fmt.Sprintf("%s %s (%s) %.4f @ %.4f | 现 %.4f | %s %.2f USDT",
		positionSideIcon(pos.Side),
		symbol,
		positionSideLabel(pos.Side),
		pos.PositionAmt,
		pos.EntryPrice,
		pos.MarkPrice,
		pnlIndicator(pos.UnrealizedProfit),
		pos.UnrealizedProfit,
	)

	if pos.Leverage > 0 {
		line += fmt.Sprintf(" | 杠杆 %.1fx", pos.Leverage)
	}

	return line
}

func positionSideIcon(side string) string {
	switch strings.ToLower(side) {
	case "long":
		return "🚀"
	case "short":
		return "📉"
	default:
		return "ℹ️"
	}
}

func positionSideLabel(side string) string {
	switch strings.ToLower(side) {
	case "long":
		return "多"
	case "short":
		return "空"
	default:
		if side == "" {
			return ""
		}
		return strings.Title(side)
	}
}

func pnlIndicator(pnl float64) string {
	switch {
	case pnl > 0:
		return "🟢"
	case pnl < 0:
		return "🔴"
	default:
		return "⚪"
	}
}

func truncateReason(text string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}

	runeText := []rune(text)
	if len(runeText) <= maxLen {
		return text
	}
	return string(runeText[:maxLen]) + "…"
}

func escapeTelegramMarkdown(text string) string {
	replacer := strings.NewReplacer(
		"_", "\\_",
		"*", "\\*",
		"[", "\\[",
		"]", "\\]",
		"(", "\\(",
		")", "\\)",
		"~", "\\~",
		"`", "\\`",
		">", "\\>",
		"#", "\\#",
		"+", "\\+",
		"-", "\\-",
		"=", "\\=",
		"|", "\\|",
		"{", "\\{",
		"}", "\\}",
		".", "\\.",
		"!", "\\!",
	)
	return replacer.Replace(text)
}

// 每日重置盈亏基线
func (at *AutoTrader) maybeResetDailyMetrics() {
	now := time.Now()
	if at.lastResetTime.IsZero() || !sameDay(at.lastResetTime, now) {
		at.dailyPnL = 0
		at.dailyPnLBase = 0
		at.needsDailyBaseline = true
		at.lastResetTime = now
		log.Println("📅 日盈亏已重置，等待新的基准净值")
	}
}

func (at *AutoTrader) enforceRiskLimits(currentEquity float64) (string, bool) {
	at.updatePnLMetrics(currentEquity)

	if limit := at.config.MaxDailyLoss; limit > 0 && at.dailyPnLBase > 0 {
		maxLoss := -at.dailyPnLBase * limit / 100
		if at.dailyPnL <= maxLoss {
			reason := fmt.Sprintf("触发当日最大亏损 %.2f%% (盈亏 %.2f / 基准 %.2f USDT)", limit, at.dailyPnL, at.dailyPnLBase)
			at.activateRiskStop()
			return reason, true
		}
	}

	if dd := at.config.MaxDrawdown; dd > 0 && at.peakEquity > 0 {
		drawdownPct := (at.peakEquity - currentEquity) / at.peakEquity * 100
		if drawdownPct >= dd {
			reason := fmt.Sprintf("触发账户回撤 %.2f%% (峰值 %.2f → 当前 %.2f)", drawdownPct, at.peakEquity, currentEquity)
			at.activateRiskStop()
			return reason, true
		}
	}

	return "", false
}

func (at *AutoTrader) updatePnLMetrics(currentEquity float64) {
	if at.dailyPnLBase == 0 || at.needsDailyBaseline {
		at.dailyPnLBase = currentEquity
		at.dailyPnL = 0
		at.needsDailyBaseline = false
		log.Printf("📊 日盈亏基准同步：%.2f USDT", currentEquity)
	} else {
		at.dailyPnL = currentEquity - at.dailyPnLBase
	}

	if currentEquity > at.peakEquity {
		at.peakEquity = currentEquity
	}
}

func (at *AutoTrader) activateRiskStop() {
	pause := at.config.StopTradingTime
	if pause <= 0 {
		pause = 60 * time.Minute
	}
	at.stopUntil = time.Now().Add(pause)
	log.Printf("⚠️ 触发风险暂停，暂停时长: %v，恢复时间: %s", pause, at.stopUntil.Format(time.RFC3339))
}

// buildTradingContext 构建交易上下文
func (at *AutoTrader) buildTradingContext() (*decision.Context, error) {
	// 1. 获取账户信息
	balance, err := at.trader.GetBalance()
	if err != nil {
		return nil, fmt.Errorf("获取账户余额失败: %w", err)
	}

	// 获取账户字段
	totalWalletBalance := 0.0
	totalUnrealizedProfit := 0.0
	availableBalance := 0.0

	if wallet, ok := balance["totalWalletBalance"].(float64); ok {
		totalWalletBalance = wallet
	}
	if unrealized, ok := balance["totalUnrealizedProfit"].(float64); ok {
		totalUnrealizedProfit = unrealized
	}
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}
	// Total Equity = 钱包余额 + 未实现盈亏
	totalEquity := totalWalletBalance + totalUnrealizedProfit

	// 2. 获取持仓信息
	positions, err := at.trader.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("获取持仓失败: %w", err)
	}

	var positionInfos []decision.PositionInfo
	totalMarginUsed := 0.0

	// 当前持仓的key集合（用于清理已平仓的记录）
	currentPositionKeys := make(map[string]bool)

	for _, pos := range positions {
		symbol := pos["symbol"].(string)
		side := pos["side"].(string)
		entryPrice := pos["entryPrice"].(float64)
		markPrice := pos["markPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity // 空仓数量为负，转为正数
		}

		// 跳过已平仓的持仓（quantity = 0），防止"幽灵持仓"传递给AI
		if quantity == 0 {
			continue
		}

		unrealizedPnl := pos["unRealizedProfit"].(float64)
		liquidationPrice := pos["liquidationPrice"].(float64)

		// 计算占用保证金（基于开仓价）
		leverage := 10 // 默认值，实际应该从持仓信息获取
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev)
		}
		marginUsed := (quantity * entryPrice) / float64(leverage)
		totalMarginUsed += marginUsed

		// 计算盈亏百分比（基于保证金，考虑杠杆）
		pnlPct := calculatePnLPercentage(unrealizedPnl, marginUsed)

		// 跟踪持仓首次出现时间
		posKey := symbol + "_" + side
		currentPositionKeys[posKey] = true
		if _, exists := at.positionFirstSeenTime[posKey]; !exists {
			// 新持仓，记录当前时间
			at.positionFirstSeenTime[posKey] = time.Now().UnixMilli()
		}
		updateTime := at.positionFirstSeenTime[posKey]

		// 获取该持仓的历史最高收益率
		at.peakPnLCacheMutex.RLock()
		peakPnlPct := at.peakPnLCache[posKey]
		at.peakPnLCacheMutex.RUnlock()

		// 获取止损止盈价格（用于后续推断平仓原因）
		stopLoss := at.positionStopLoss[posKey]
		takeProfit := at.positionTakeProfit[posKey]

		positionInfos = append(positionInfos, decision.PositionInfo{
			Symbol:           symbol,
			Side:             side,
			EntryPrice:       entryPrice,
			MarkPrice:        markPrice,
			Quantity:         quantity,
			Leverage:         leverage,
			UnrealizedPnL:    unrealizedPnl,
			UnrealizedPnLPct: pnlPct,
			PeakPnLPct:       peakPnlPct,
			LiquidationPrice: liquidationPrice,
			MarginUsed:       marginUsed,
			UpdateTime:       updateTime,
			StopLoss:         stopLoss,
			TakeProfit:       takeProfit,
		})
	}

	// 清理已平仓的持仓记录（包括止损止盈记录）
	for key := range at.positionFirstSeenTime {
		if !currentPositionKeys[key] {
			delete(at.positionFirstSeenTime, key)
			delete(at.positionStopLoss, key)
			delete(at.positionTakeProfit, key)
		}
	}

	// 3. 获取交易员的候选币种池
	candidateCoins, err := at.getCandidateCoins()
	if err != nil {
		return nil, fmt.Errorf("获取候选币种失败: %w", err)
	}

	// 4. 计算总盈亏
	totalPnL := totalEquity - at.initialBalance
	totalPnLPct := 0.0
	if at.initialBalance > 0 {
		totalPnLPct = (totalPnL / at.initialBalance) * 100
	}

	marginUsedPct := 0.0
	if totalEquity > 0 {
		marginUsedPct = (totalMarginUsed / totalEquity) * 100
	}

	// 5. 分析历史表现（最近100个周期，避免长期持仓的交易记录丢失）
	// 假设每3分钟一个周期，100个周期 = 5小时，足够覆盖大部分交易
	performance, err := at.decisionLogger.AnalyzePerformance(100)
	if err != nil {
		log.Printf("⚠️  分析历史表现失败: %v", err)
		// 不影响主流程，继续执行（但设置performance为nil以避免传递错误数据）
		performance = nil
	}

	// 6. Fetch open orders for AI decision context to prevent duplicate orders
	openOrders, err := at.trader.GetOpenOrders("")
	if err != nil {
		log.Printf("⚠️  Failed to fetch open orders: %v (continuing execution, but AI won't see order status)", err)
		// Don't block main flow, use empty list
		openOrders = []decision.OpenOrderInfo{}
	} else {
		log.Printf("  ✓ Fetched %d open orders", len(openOrders))
	}

	// 7. Build context
	ctx := &decision.Context{
		CurrentTime:     time.Now().Format("2006-01-02 15:04:05"),
		RuntimeMinutes:  int(time.Since(at.startTime).Minutes()),
		CallCount:       at.callCount,
		BTCETHLeverage:  at.config.BTCETHLeverage,  // 使用配置的杠杆倍数
		AltcoinLeverage: at.config.AltcoinLeverage, // 使用配置的杠杆倍数
		TakerFeeRate:    at.config.TakerFeeRate,    // Use configured taker fee rate
		MakerFeeRate:    at.config.MakerFeeRate,    // Use configured maker fee rate
		Timeframes:      at.timeframes,             // K线时间线配置
		Account: decision.AccountInfo{
			TotalEquity:      totalEquity,
			AvailableBalance: availableBalance,
			UnrealizedPnL:    totalUnrealizedProfit,
			TotalPnL:         totalPnL,
			TotalPnLPct:      totalPnLPct,
			MarginUsed:       totalMarginUsed,
			MarginUsedPct:    marginUsedPct,
			PositionCount:    len(positionInfos),
		},
		Positions:      positionInfos,
		OpenOrders:     openOrders, // 添加未成交订单（用于 AI 了解挂单状态，避免重复下单）
		CandidateCoins: candidateCoins,
		Performance:    performance, // 添加历史表现分析（包含 RecentTrades 用于 AI 学习）
	}

	return ctx, nil
}

// executeDecisionWithRecord 执行AI决策并记录详细信息
func (at *AutoTrader) executeDecisionWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	switch decision.Action {
	case "open_long":
		return at.executeOpenLongWithRecord(decision, actionRecord)
	case "open_short":
		return at.executeOpenShortWithRecord(decision, actionRecord)
	case "close_long":
		return at.executeCloseLongWithRecord(decision, actionRecord)
	case "close_short":
		return at.executeCloseShortWithRecord(decision, actionRecord)
	case "update_stop_loss":
		return at.executeUpdateStopLossWithRecord(decision, actionRecord)
	case "update_take_profit":
		return at.executeUpdateTakeProfitWithRecord(decision, actionRecord)
	case "partial_close":
		return at.executePartialCloseWithRecord(decision, actionRecord)
	case "hold", "wait":
		// 无需执行，仅记录
		return nil
	default:
		return fmt.Errorf("未知的action: %s", decision.Action)
	}
}

// executeOpenLongWithRecord 执行开多仓并记录详细信息
func (at *AutoTrader) executeOpenLongWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  📈 开多仓: %s", decision.Symbol)

	// ⚠️ 关键：检查是否已有同币种同方向持仓，如果有则拒绝开仓（防止仓位叠加超限）
	positions, err := at.trader.GetPositions()
	if err == nil {
		for _, pos := range positions {
			if pos["symbol"] == decision.Symbol && pos["side"] == "long" {
				return fmt.Errorf("❌ %s 已有多仓，拒绝开仓以防止仓位叠加超限。如需换仓，请先给出 close_long 决策", decision.Symbol)
			}
		}
	}

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol, at.timeframes)
	if err != nil {
		return err
	}

	// 🔍 价格一致性验证（防止单交易所价格异常导致误判）
	if market.WSMonitorCli != nil && market.WSMonitorCli.GetDSManager() != nil {
		consistent, prices, err := market.WSMonitorCli.GetDSManager().VerifyPriceConsistency(decision.Symbol, 0.02) // 2% 偏差阈值
		if err != nil {
			log.Printf("⚠️  %s 价格验证失败（数据源不足），继续交易: %v", decision.Symbol, err)
		} else if !consistent {
			priceDetails := ""
			for source, price := range prices {
				priceDetails += fmt.Sprintf("%s: %.2f, ", source, price)
			}
			return fmt.Errorf("❌ 价格异常：%s 在多个数据源间偏差过大（>2%%），拒绝开仓以防止误判。价格: %s",
				decision.Symbol, priceDetails)
		} else {
			log.Printf("✅ %s 价格验证通过（多数据源一致性检查）", decision.Symbol)
		}
	}

	balance, err := at.trader.GetBalance()
	if err != nil {
		return fmt.Errorf("获取账户余额失败: %w", err)
	}
	availableBalance := 0.0
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}
	accountEquity := at.estimateAccountEquity(balance, availableBalance)

	feeRate := at.config.TakerFeeRate
	if feeRate <= 0 {
		feeRate = 0.0004
	}

	positionSizeUSD, _, _, _, err := at.preparePositionSizing(decision, availableBalance, feeRate)
	if err != nil {
		return err
	}

	quantity := positionSizeUSD / marketData.CurrentPrice
	if quantity <= 0 {
		return fmt.Errorf("无效的开仓数量: %.4f", quantity)
	}

	actionRecord.Quantity = quantity
	actionRecord.Price = marketData.CurrentPrice

	// ⚡ 严格验证止损/止盈价格（防止开仓后无法设置保护，导致仓位风险）
	// 修复 Issue: 开仓成功但止损/止盈设置失败，仓位失去保护
	if decision.StopLoss <= 0 || decision.TakeProfit <= 0 {
		return fmt.Errorf("❌ 多单开仓失败：止损价 %.2f 和止盈价 %.2f 必须大于 0。"+
			"建议：AI 必须为每个开仓决策设置合理的止损和止盈价格",
			decision.StopLoss, decision.TakeProfit)
	}

	// 多单：止损必须 < 当前价，止盈必须 > 当前价
	if decision.StopLoss >= marketData.CurrentPrice {
		priceGapPct := ((decision.StopLoss - marketData.CurrentPrice) / marketData.CurrentPrice) * 100
		return fmt.Errorf("❌ 多单止损价异常偏高：止损价 %.2f 必须低于当前价 %.2f (当前高出 %.2f%%)。"+
			"建议：AI 应设置低于当前价的止损价，例如 %.2f",
			decision.StopLoss, marketData.CurrentPrice, priceGapPct, marketData.CurrentPrice*0.98)
	}

	if decision.TakeProfit <= marketData.CurrentPrice {
		priceGapPct := ((marketData.CurrentPrice - decision.TakeProfit) / marketData.CurrentPrice) * 100
		return fmt.Errorf("❌ 多单止盈价异常偏低：止盈价 %.2f 必须高于当前价 %.2f (当前低于 %.2f%%)。"+
			"建议：AI 应设置高于当前价的止盈价，例如 %.2f",
			decision.TakeProfit, marketData.CurrentPrice, priceGapPct, marketData.CurrentPrice*1.02)
	}

	// 设置仓位模式
	if err := at.trader.SetMarginMode(decision.Symbol, at.config.IsCrossMargin); err != nil {
		log.Printf("  ⚠️ 设置仓位模式失败: %v", err)
		// 继续执行，不影响交易
	}

	// 开仓
	order, err := at.trader.OpenLong(decision.Symbol, quantity, decision.Leverage)
	if err != nil && isMarginInsufficientError(err) {
		log.Printf("  ⚠️ %s 开多仓保证金不足: %v，尝试刷新余额并降额重试", decision.Symbol, err)
		order, err = at.retryOpenPositionWithReducedSize(decision, actionRecord, marketData.CurrentPrice, availableBalance, feeRate, func(q float64) (map[string]interface{}, error) {
			return at.trader.OpenLong(decision.Symbol, q, decision.Leverage)
		})
		if err == nil && actionRecord.Quantity > 0 {
			quantity = actionRecord.Quantity
		}
	}
	if err != nil {
		return err
	}

	// 记录订单ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	at.annotateRisk(decision, actionRecord, "LONG", quantity, actionRecord.Price, decision.StopLoss, accountEquity)

	log.Printf("  ✓ 开仓成功，订单ID: %v, 数量: %.4f", order["orderId"], quantity)

	// 记录开仓时间
	posKey := decision.Symbol + "_long"
	at.positionFirstSeenTime[posKey] = time.Now().UnixMilli()

	// 设置止损止盈
	if err := at.trader.SetStopLoss(decision.Symbol, "LONG", quantity, decision.StopLoss); err != nil {
		log.Printf("  ⚠ 设置止损失败: %v", err)
	} else {
		at.positionStopLoss[posKey] = decision.StopLoss // 记录止损价格
	}
	if err := at.trader.SetTakeProfit(decision.Symbol, "LONG", quantity, decision.TakeProfit); err != nil {
		log.Printf("  ⚠ 设置止盈失败: %v", err)
	} else {
		at.positionTakeProfit[posKey] = decision.TakeProfit // 记录止盈价格
	}

	return nil
}

// executeOpenShortWithRecord 执行开空仓并记录详细信息
func (at *AutoTrader) executeOpenShortWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  📉 开空仓: %s", decision.Symbol)

	// ⚠️ 关键：检查是否已有同币种同方向持仓，如果有则拒绝开仓（防止仓位叠加超限）
	positions, err := at.trader.GetPositions()
	if err == nil {
		for _, pos := range positions {
			if pos["symbol"] == decision.Symbol && pos["side"] == "short" {
				return fmt.Errorf("❌ %s 已有空仓，拒绝开仓以防止仓位叠加超限。如需换仓，请先给出 close_short 决策", decision.Symbol)
			}
		}
	}

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol, at.timeframes)
	if err != nil {
		return err
	}

	// 🔍 价格一致性验证（防止单交易所价格异常导致误判）
	if market.WSMonitorCli != nil && market.WSMonitorCli.GetDSManager() != nil {
		consistent, prices, err := market.WSMonitorCli.GetDSManager().VerifyPriceConsistency(decision.Symbol, 0.02) // 2% 偏差阈值
		if err != nil {
			log.Printf("⚠️  %s 价格验证失败（数据源不足），继续交易: %v", decision.Symbol, err)
		} else if !consistent {
			priceDetails := ""
			for source, price := range prices {
				priceDetails += fmt.Sprintf("%s: %.2f, ", source, price)
			}
			return fmt.Errorf("❌ 价格异常：%s 在多个数据源间偏差过大（>2%%），拒绝开仓以防止误判。价格: %s",
				decision.Symbol, priceDetails)
		} else {
			log.Printf("✅ %s 价格验证通过（多数据源一致性检查）", decision.Symbol)
		}
	}

	balance, err := at.trader.GetBalance()
	if err != nil {
		return fmt.Errorf("获取账户余额失败: %w", err)
	}
	availableBalance := 0.0
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}
	accountEquity := at.estimateAccountEquity(balance, availableBalance)

	feeRate := at.config.TakerFeeRate
	if feeRate <= 0 {
		feeRate = 0.0004
	}

	positionSizeUSD, _, _, _, err := at.preparePositionSizing(decision, availableBalance, feeRate)
	if err != nil {
		return err
	}

	quantity := positionSizeUSD / marketData.CurrentPrice
	if quantity <= 0 {
		return fmt.Errorf("无效的开仓数量: %.4f", quantity)
	}

	actionRecord.Quantity = quantity
	actionRecord.Price = marketData.CurrentPrice

	// ⚡ 严格验证止损/止盈价格（防止开仓后无法设置保护，导致仓位风险）
	// 修复 Issue: 开仓成功但止损/止盈设置失败，仓位失去保护
	if decision.StopLoss <= 0 || decision.TakeProfit <= 0 {
		return fmt.Errorf("❌ 空单开仓失败：止损价 %.2f 和止盈价 %.2f 必须大于 0。"+
			"建议：AI 必须为每个开仓决策设置合理的止损和止盈价格",
			decision.StopLoss, decision.TakeProfit)
	}

	// 空单：止损必须 > 当前价，止盈必须 < 当前价
	if decision.StopLoss <= marketData.CurrentPrice {
		priceGapPct := ((marketData.CurrentPrice - decision.StopLoss) / marketData.CurrentPrice) * 100
		return fmt.Errorf("❌ 空单止损价不合理：止损价 %.2f 必须高于当前价 %.2f (当前低于 %.2f%%)。"+
			"建议：AI 应设置高于当前价的止损价，例如 %.2f",
			decision.StopLoss, marketData.CurrentPrice, priceGapPct, marketData.CurrentPrice*1.02)
	}

	if decision.TakeProfit >= marketData.CurrentPrice {
		priceGapPct := ((decision.TakeProfit - marketData.CurrentPrice) / marketData.CurrentPrice) * 100
		return fmt.Errorf("❌ 空单止盈价不合理：止盈价 %.2f 必须低于当前价 %.2f (当前高出 %.2f%%)。"+
			"建议：AI 应设置低于当前价的止盈价，例如 %.2f",
			decision.TakeProfit, marketData.CurrentPrice, priceGapPct, marketData.CurrentPrice*0.98)
	}

	// 设置仓位模式
	if err := at.trader.SetMarginMode(decision.Symbol, at.config.IsCrossMargin); err != nil {
		log.Printf("  ⚠️ 设置仓位模式失败: %v", err)
		// 继续执行，不影响交易
	}

	// 开仓
	order, err := at.trader.OpenShort(decision.Symbol, quantity, decision.Leverage)
	if err != nil && isMarginInsufficientError(err) {
		log.Printf("  ⚠️ %s 开空仓保证金不足: %v，尝试刷新余额并降额重试", decision.Symbol, err)
		order, err = at.retryOpenPositionWithReducedSize(decision, actionRecord, marketData.CurrentPrice, availableBalance, feeRate, func(q float64) (map[string]interface{}, error) {
			return at.trader.OpenShort(decision.Symbol, q, decision.Leverage)
		})
		if err == nil && actionRecord.Quantity > 0 {
			quantity = actionRecord.Quantity
		}
	}
	if err != nil {
		return err
	}

	// 记录订单ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	at.annotateRisk(decision, actionRecord, "SHORT", quantity, actionRecord.Price, decision.StopLoss, accountEquity)

	log.Printf("  ✓ 开仓成功，订单ID: %v, 数量: %.4f", order["orderId"], quantity)

	// 记录开仓时间
	posKey := decision.Symbol + "_short"
	at.positionFirstSeenTime[posKey] = time.Now().UnixMilli()

	// 设置止损止盈
	if err := at.trader.SetStopLoss(decision.Symbol, "SHORT", quantity, decision.StopLoss); err != nil {
		log.Printf("  ⚠ 设置止损失败: %v", err)
	} else {
		at.positionStopLoss[posKey] = decision.StopLoss // 记录止损价格
	}
	if err := at.trader.SetTakeProfit(decision.Symbol, "SHORT", quantity, decision.TakeProfit); err != nil {
		log.Printf("  ⚠ 设置止盈失败: %v", err)
	} else {
		at.positionTakeProfit[posKey] = decision.TakeProfit // 记录止盈价格
	}

	return nil
}

func (at *AutoTrader) preparePositionSizing(decision *decision.Decision, availableBalance, feeRate float64) (positionSizeUSD, requiredMargin, estimatedFee, totalRequired float64, err error) {
	leverage := float64(decision.Leverage)
	if leverage <= 0 {
		return 0, 0, 0, 0, fmt.Errorf("无效的杠杆倍数: %d", decision.Leverage)
	}

	positionSizeUSD = decision.PositionSizeUSD
	requiredMargin = positionSizeUSD / leverage
	estimatedFee = positionSizeUSD * feeRate
	totalRequired = requiredMargin + estimatedFee

	if totalRequired <= availableBalance {
		return positionSizeUSD, requiredMargin, estimatedFee, totalRequired, nil
	}

	denominator := (1.0 / leverage) + feeRate
	if denominator <= 0 {
		return 0, 0, 0, 0, fmt.Errorf("无效的保证金或手续费配置（杠杆=%d, 手续费率=%.6f）", decision.Leverage, feeRate)
	}

	maxAffordableUSD := availableBalance / denominator
	adjustedPositionUSD := maxAffordableUSD * 0.995

	if adjustedPositionUSD <= 0 {
		return 0, requiredMargin, estimatedFee, totalRequired,
			fmt.Errorf("❌ 保证金不足: 需要 %.2f USDT（保证金 %.2f + 手续费 %.2f），可用 %.2f USDT",
				totalRequired, requiredMargin, estimatedFee, availableBalance)
	}

	if adjustedPositionUSD < positionSizeUSD {
		log.Printf("  ⚠️ 可用余额 %.2f USDT 不足以支撑计划开仓 %.2f USDT，将自动降额至 %.2f USDT（包含手续费缓冲）",
			availableBalance, positionSizeUSD, adjustedPositionUSD)
		positionSizeUSD = adjustedPositionUSD
		requiredMargin = positionSizeUSD / leverage
		estimatedFee = positionSizeUSD * feeRate
		totalRequired = requiredMargin + estimatedFee
	}

	if totalRequired > availableBalance {
		safetyAvailable := availableBalance * 0.995
		if safetyAvailable <= 0 {
			return 0, requiredMargin, estimatedFee, totalRequired,
				fmt.Errorf("❌ 保证金不足: 需要 %.2f USDT（保证金 %.2f + 手续费 %.2f），可用 %.2f USDT",
					totalRequired, requiredMargin, estimatedFee, availableBalance)
		}

		positionSizeUSD = math.Min(positionSizeUSD, safetyAvailable/denominator)
		if positionSizeUSD <= 0 {
			return 0, requiredMargin, estimatedFee, totalRequired,
				fmt.Errorf("❌ 保证金不足: 需要 %.2f USDT（保证金 %.2f + 手续费 %.2f），可用 %.2f USDT",
					totalRequired, requiredMargin, estimatedFee, availableBalance)
		}

		requiredMargin = positionSizeUSD / leverage
		estimatedFee = positionSizeUSD * feeRate
		totalRequired = requiredMargin + estimatedFee

		if totalRequired > availableBalance {
			return 0, requiredMargin, estimatedFee, totalRequired,
				fmt.Errorf("❌ 保证金不足: 需要 %.2f USDT（保证金 %.2f + 手续费 %.2f），可用 %.2f USDT",
					totalRequired, requiredMargin, estimatedFee, availableBalance)
		}
	}

	return positionSizeUSD, requiredMargin, estimatedFee, totalRequired, nil
}

type balanceCacheInvalidator interface {
	InvalidateBalanceCache()
}

func isMarginInsufficientError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "Margin is insufficient") || strings.Contains(errMsg, "-2019")
}

func (at *AutoTrader) retryOpenPositionWithReducedSize(
	decision *decision.Decision,
	actionRecord *logger.DecisionAction,
	currentPrice float64,
	availableBalance float64,
	feeRate float64,
	orderFunc func(quantity float64) (map[string]interface{}, error),
) (map[string]interface{}, error) {
	freshAvailable := at.refreshAvailableBalance(availableBalance)
	safetyAvailable := freshAvailable * 0.97 // 预留 3% 缓冲，防止紧贴可用保证金
	if safetyAvailable <= 0 {
		return nil, fmt.Errorf("❌ 保证金不足: 实时可用余额 %.2f USDT，无法安全开仓", freshAvailable)
	}

	positionSizeUSD, _, _, _, err := at.preparePositionSizing(decision, safetyAvailable, feeRate)
	if err != nil {
		return nil, err
	}

	quantity := positionSizeUSD / currentPrice
	if quantity <= 0 {
		return nil, fmt.Errorf("调整后开仓数量无效: %.6f", quantity)
	}

	log.Printf("  ↘️ 自动降额至 %.2f USDT（数量 %.6f）后重新尝试开仓", positionSizeUSD, quantity)
	actionRecord.Quantity = quantity
	return orderFunc(quantity)
}

func (at *AutoTrader) refreshAvailableBalance(current float64) float64 {
	if invalidator, ok := at.trader.(balanceCacheInvalidator); ok {
		invalidator.InvalidateBalanceCache()
	}

	balance, err := at.trader.GetBalance()
	if err != nil {
		log.Printf("  ⚠️ 刷新账户余额失败: %v（沿用当前可用 %.2f USDT）", err, current)
		return current
	}

	if avail, ok := balance["availableBalance"].(float64); ok && avail > 0 {
		log.Printf("  ↺ 实时可用余额更新为 %.2f USDT", avail)
		return avail
	}

	log.Printf("  ⚠️ 账户余额缺少 availableBalance 字段（沿用 %.2f USDT）", current)
	return current
}

// executeCloseLongWithRecord 执行平多仓并记录详细信息
func (at *AutoTrader) executeCloseLongWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  🔄 平多仓: %s", decision.Symbol)

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol, at.timeframes)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// 平仓（失败自动退化为市价兜底）
	order, err := at.trader.CloseLong(decision.Symbol, 0) // 0 = 全部平仓
	if err != nil {
		log.Printf("  ⚠️ 平多仓失败: %v，尝试取消挂单并改用市价单", err)
		if cancelErr := at.trader.CancelAllOrders(decision.Symbol); cancelErr != nil {
			log.Printf("  ⚠️ 取消挂单失败: %v", cancelErr)
		}
		order, err = at.trader.CloseLong(decision.Symbol, 0)
		if err != nil {
			return err
		}
		log.Printf("  ✓ 平多仓改用市价单成功")
	}

	// 记录订单ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	log.Printf("  ✓ 平仓成功")
	return nil
}

// executeCloseShortWithRecord 执行平空仓并记录详细信息
func (at *AutoTrader) executeCloseShortWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  🔄 平空仓: %s", decision.Symbol)

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol, at.timeframes)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// 平仓（失败自动退化为市价兜底）
	order, err := at.trader.CloseShort(decision.Symbol, 0) // 0 = 全部平仓
	if err != nil {
		log.Printf("  ⚠️ 平空仓失败: %v，尝试取消挂单并改用市价单", err)
		if cancelErr := at.trader.CancelAllOrders(decision.Symbol); cancelErr != nil {
			log.Printf("  ⚠️ 取消挂单失败: %v", cancelErr)
		}
		order, err = at.trader.CloseShort(decision.Symbol, 0)
		if err != nil {
			return err
		}
		log.Printf("  ✓ 平空仓改用市价单成功")
	}

	// 记录订单ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	log.Printf("  ✓ 平仓成功")
	return nil
}

// executeUpdateStopLossWithRecord 执行调整止损并记录详细信息
func (at *AutoTrader) executeUpdateStopLossWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  🎯 调整止损: %s → %.2f", decision.Symbol, decision.NewStopLoss)

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol, at.timeframes)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// 获取当前持仓
	positions, err := at.trader.GetPositions()
	if err != nil {
		return fmt.Errorf("获取持仓失败: %w", err)
	}

	// 查找目标持仓
	var targetPosition map[string]interface{}
	for _, pos := range positions {
		symbol, _ := pos["symbol"].(string)
		posAmt, _ := pos["positionAmt"].(float64)
		if symbol == decision.Symbol && posAmt != 0 {
			targetPosition = pos
			break
		}
	}

	// ⚡ 方案 A：智能止损验证 - 检测持仓是否已被交易所自动平仓
	if targetPosition == nil {
		// 检查这个持仓是否在上一个周期存在（说明刚刚被平仓）
		wasRecentlyOpen := false
		for key := range at.lastPositions {
			if strings.HasPrefix(key, decision.Symbol+"_") {
				wasRecentlyOpen = true
				break
			}
		}

		if wasRecentlyOpen {
			// 持仓刚刚消失，很可能是止损单已触发
			log.Printf("  ℹ️  %s 持仓已平仓（止损单可能已触发），跳过止损调整", decision.Symbol)
			log.Printf("  💡 提示：市价 %.2f，目标止损 %.2f - 交易所可能已在两次AI周期间执行止损",
				marketData.CurrentPrice, decision.NewStopLoss)
			return nil // 优雅返回，不抛错误
		}

		// 如果从未存在过这个持仓，则是配置错误
		return fmt.Errorf("持仓不存在: %s（从未开仓或已在更早前平仓）", decision.Symbol)
	}

	// 获取持仓方向和数量
	side, _ := targetPosition["side"].(string)
	positionSide := strings.ToUpper(side)
	positionAmt, _ := targetPosition["positionAmt"].(float64)

	// ⚡ 严格验证新止损价格合理性（防止 "Order would immediately trigger" 错误）
	priceGap := 0.0
	if positionSide == "LONG" {
		priceGap = decision.NewStopLoss - marketData.CurrentPrice
		if priceGap > 0 {
			// ❌ 多单止损价高于当前价 - 会立即触发，交易所会拒绝
			return fmt.Errorf("多单止损价异常偏高：多单止损必须低于当前价格 (当前: %.2f, 止损: %.2f)",
				marketData.CurrentPrice, decision.NewStopLoss)
		}
	} else {
		priceGap = marketData.CurrentPrice - decision.NewStopLoss
		if priceGap > 0 {
			// ❌ 空单止损价低于当前价 - 会立即触发，交易所会拒绝
			return fmt.Errorf("空单止损必须高于当前价格 (当前: %.2f, 止损: %.2f)",
				marketData.CurrentPrice, decision.NewStopLoss)
		}
	}

	// ⚠️ 防御性检查：检测是否存在双向持仓（不应该出现，但提供保护）
	var hasOppositePosition bool
	oppositeSide := ""
	for _, pos := range positions {
		symbol, _ := pos["symbol"].(string)
		posSide, _ := pos["side"].(string)
		posAmt, _ := pos["positionAmt"].(float64)
		if symbol == decision.Symbol && posAmt != 0 && strings.ToUpper(posSide) != positionSide {
			hasOppositePosition = true
			oppositeSide = strings.ToUpper(posSide)
			break
		}
	}

	if hasOppositePosition {
		log.Printf("  🚨 警告：检测到 %s 存在双向持仓（%s + %s），这违反了策略规则",
			decision.Symbol, positionSide, oppositeSide)
		log.Printf("  🚨 取消止损单将影响两个方向的订单，请检查是否为用户手动操作导致")
		log.Printf("  🚨 建议：手动平掉其中一个方向的持仓，或检查系统是否有BUG")
	}

	// 取消旧的止损单（只删除止损单，不影响止盈单）
	// 注意：如果存在双向持仓，这会删除两个方向的止损单
	// ✅ 修复 Issue #998: 必须成功取消旧单才能继续，防止重复挂单
	if err := at.trader.CancelStopLossOrders(decision.Symbol); err != nil {
		return fmt.Errorf("取消舊止損單失敗，中止操作以防止重複掛單 (Issue #998): %w", err)
	}

	log.Printf("  ✓ 已取消舊止損單，準備設置新止損")

	// 调用交易所 API 修改止损
	quantity := math.Abs(positionAmt)
	err = at.trader.SetStopLoss(decision.Symbol, positionSide, quantity, decision.NewStopLoss)
	if err != nil {
		return fmt.Errorf("修改止损失败: %w", err)
	}

	log.Printf("  ✓ 止损已调整: %.2f (当前价格: %.2f)", decision.NewStopLoss, marketData.CurrentPrice)
	return nil
}

// executeUpdateTakeProfitWithRecord 执行调整止盈并记录详细信息
func (at *AutoTrader) executeUpdateTakeProfitWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  🎯 调整止盈: %s → %.2f", decision.Symbol, decision.NewTakeProfit)

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol, at.timeframes)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// 获取当前持仓
	positions, err := at.trader.GetPositions()
	if err != nil {
		return fmt.Errorf("获取持仓失败: %w", err)
	}

	// 查找目标持仓
	var targetPosition map[string]interface{}
	for _, pos := range positions {
		symbol, _ := pos["symbol"].(string)
		posAmt, _ := pos["positionAmt"].(float64)
		if symbol == decision.Symbol && posAmt != 0 {
			targetPosition = pos
			break
		}
	}

	// ⚡ 方案 A：智能止盈验证 - 检测持仓是否已被交易所自动平仓
	if targetPosition == nil {
		// 检查这个持仓是否在上一个周期存在（说明刚刚被平仓）
		wasRecentlyOpen := false
		for key := range at.lastPositions {
			if strings.HasPrefix(key, decision.Symbol+"_") {
				wasRecentlyOpen = true
				break
			}
		}

		if wasRecentlyOpen {
			// 持仓刚刚消失，很可能是止盈单已触发
			log.Printf("  ℹ️  %s 持仓已平仓（止盈单可能已触发），跳过止盈调整", decision.Symbol)
			log.Printf("  💡 提示：市价 %.2f，目标止盈 %.2f - 交易所可能已在两次AI周期间执行止盈",
				marketData.CurrentPrice, decision.NewTakeProfit)
			return nil // 优雅返回，不抛错误
		}

		// 如果从未存在过这个持仓，则是配置错误
		return fmt.Errorf("持仓不存在: %s（从未开仓或已在更早前平仓）", decision.Symbol)
	}

	// 获取持仓方向和数量
	side, _ := targetPosition["side"].(string)
	positionSide := strings.ToUpper(side)
	positionAmt, _ := targetPosition["positionAmt"].(float64)

	// ⚡ 严格验证新止盈价格合理性（防止 "Order would immediately trigger" 错误）
	priceGap := 0.0
	if positionSide == "LONG" {
		priceGap = marketData.CurrentPrice - decision.NewTakeProfit
		if priceGap > 0 {
			// ❌ 多单止盈价低于当前价 - 会立即触发，交易所会拒绝
			return fmt.Errorf("多单止盈价异常偏低：多单止盈必须高于当前价格 (当前: %.2f, 止盈: %.2f)",
				marketData.CurrentPrice, decision.NewTakeProfit)
		}
	} else {
		priceGap = decision.NewTakeProfit - marketData.CurrentPrice
		if priceGap > 0 {
			// ❌ 空单止盈价高于当前价 - 会立即触发，交易所会拒绝
			return fmt.Errorf("空单止盈必须低于当前价格 (当前: %.2f, 止盈: %.2f)",
				marketData.CurrentPrice, decision.NewTakeProfit)
		}
	}

	// ⚠️ 防御性检查：检测是否存在双向持仓（不应该出现，但提供保护）
	var hasOppositePosition bool
	oppositeSide := ""
	for _, pos := range positions {
		symbol, _ := pos["symbol"].(string)
		posSide, _ := pos["side"].(string)
		posAmt, _ := pos["positionAmt"].(float64)
		if symbol == decision.Symbol && posAmt != 0 && strings.ToUpper(posSide) != positionSide {
			hasOppositePosition = true
			oppositeSide = strings.ToUpper(posSide)
			break
		}
	}

	if hasOppositePosition {
		log.Printf("  🚨 警告：检测到 %s 存在双向持仓（%s + %s），这违反了策略规则",
			decision.Symbol, positionSide, oppositeSide)
		log.Printf("  🚨 取消止盈单将影响两个方向的订单，请检查是否为用户手动操作导致")
		log.Printf("  🚨 建议：手动平掉其中一个方向的持仓，或检查系统是否有BUG")
	}

	// 取消旧的止盈单（只删除止盈单，不影响止损单）
	// 注意：如果存在双向持仓，这会删除两个方向的止盈单
	// ✅ 修复 Issue #998: 必须成功取消旧单才能继续，防止重复挂单
	if err := at.trader.CancelTakeProfitOrders(decision.Symbol); err != nil {
		return fmt.Errorf("取消舊止盈單失敗，中止操作以防止重複掛單 (Issue #998): %w", err)
	}

	log.Printf("  ✓ 已取消舊止盈單，準備設置新止盈")

	// 调用交易所 API 修改止盈
	quantity := math.Abs(positionAmt)
	err = at.trader.SetTakeProfit(decision.Symbol, positionSide, quantity, decision.NewTakeProfit)
	if err != nil {
		return fmt.Errorf("修改止盈失败: %w", err)
	}

	log.Printf("  ✓ 止盈已调整: %.2f (当前价格: %.2f)", decision.NewTakeProfit, marketData.CurrentPrice)
	return nil
}

// executePartialCloseWithRecord 执行部分平仓并记录详细信息
func (at *AutoTrader) executePartialCloseWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  📊 部分平仓: %s %.1f%%", decision.Symbol, decision.ClosePercentage)

	// 验证百分比范围
	if decision.ClosePercentage <= 0 || decision.ClosePercentage > 100 {
		return fmt.Errorf("平仓百分比必须在 0-100 之间，当前: %.1f", decision.ClosePercentage)
	}

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol, at.timeframes)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// 获取当前持仓
	positions, err := at.trader.GetPositions()
	if err != nil {
		return fmt.Errorf("获取持仓失败: %w", err)
	}

	// 查找目标持仓
	var targetPosition map[string]interface{}
	for _, pos := range positions {
		symbol, _ := pos["symbol"].(string)
		posAmt, _ := pos["positionAmt"].(float64)
		if symbol == decision.Symbol && posAmt != 0 {
			targetPosition = pos
			break
		}
	}

	// ⚡ 方案 A：智能部分平仓验证 - 检测持仓是否已被交易所自动平仓
	if targetPosition == nil {
		// 检查这个持仓是否在上一个周期存在（说明刚刚被平仓）
		wasRecentlyOpen := false
		for key := range at.lastPositions {
			if strings.HasPrefix(key, decision.Symbol+"_") {
				wasRecentlyOpen = true
				break
			}
		}

		if wasRecentlyOpen {
			// 持仓刚刚消失，很可能是止损/止盈单已触发全部平仓
			log.Printf("  ℹ️  %s 持仓已完全平仓（止损/止盈可能已触发），跳过部分平仓", decision.Symbol)
			log.Printf("  💡 提示：市价 %.2f - 交易所可能已在两次AI周期间自动平仓",
				marketData.CurrentPrice)
			return nil // 优雅返回，不抛错误
		}

		// 如果从未存在过这个持仓，则是配置错误
		return fmt.Errorf("持仓不存在: %s（从未开仓或已在更早前平仓）", decision.Symbol)
	}

	// 获取持仓方向和数量
	side, _ := targetPosition["side"].(string)
	positionSide := strings.ToUpper(side)
	positionAmt, _ := targetPosition["positionAmt"].(float64)

	// 计算平仓数量
	totalQuantity := math.Abs(positionAmt)
	closeQuantity := totalQuantity * (decision.ClosePercentage / 100.0)
	actionRecord.Quantity = closeQuantity

	// ✅ Layer 2: 最小仓位检查（防止产生小额剩余）
	markPrice, ok := targetPosition["markPrice"].(float64)
	if !ok || markPrice <= 0 {
		return fmt.Errorf("无法解析当前价格，无法执行最小仓位检查")
	}

	currentPositionValue := totalQuantity * markPrice
	remainingQuantity := totalQuantity - closeQuantity
	remainingValue := remainingQuantity * markPrice

	const MIN_POSITION_VALUE = 10.0 // 最小持仓价值 10 USDT（對齊交易所底线，小仓位建议直接全平）

	if remainingValue > 0 && remainingValue <= MIN_POSITION_VALUE {
		log.Printf("⚠️ 检测到 partial_close 后剩余仓位 %.2f USDT < %.0f USDT",
			remainingValue, MIN_POSITION_VALUE)
		log.Printf("  → 当前仓位价值: %.2f USDT, 平仓 %.1f%%, 剩余: %.2f USDT",
			currentPositionValue, decision.ClosePercentage, remainingValue)
		log.Printf("  → 自动修正为全部平仓，避免产生无法平仓的小额剩余")

		// 🔄 自动修正为全部平仓
		if positionSide == "LONG" {
			decision.Action = "close_long"
			log.Printf("  ✓ 已修正为: close_long")
			return at.executeCloseLongWithRecord(decision, actionRecord)
		} else {
			decision.Action = "close_short"
			log.Printf("  ✓ 已修正为: close_short")
			return at.executeCloseShortWithRecord(decision, actionRecord)
		}
	}

	// 执行平仓
	var order map[string]interface{}
	if positionSide == "LONG" {
		order, err = at.trader.CloseLong(decision.Symbol, closeQuantity)
	} else {
		order, err = at.trader.CloseShort(decision.Symbol, closeQuantity)
	}

	if err != nil {
		return fmt.Errorf("部分平仓失败: %w", err)
	}

	// 记录订单ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	log.Printf("  ✓ 部分平仓成功: 平仓 %.4f (%.1f%%), 剩余 %.4f",
		closeQuantity, decision.ClosePercentage, remainingQuantity)

	// ✅ Step 4: Restore TP/SL protection (prevent remaining position from being unprotected)
	// IMPORTANT: Exchanges like Binance automatically cancel existing TP/SL orders after partial close (due to quantity mismatch)
	// If AI provides new stop-loss/take-profit prices, reset protection for the remaining position
	if decision.NewStopLoss > 0 {
		// ⚡ 验证止损价格合理性（防止 code=-2021 错误）
		isValidStopLoss := false
		if positionSide == "LONG" && decision.NewStopLoss < marketData.CurrentPrice {
			isValidStopLoss = true
		} else if positionSide == "SHORT" && decision.NewStopLoss > marketData.CurrentPrice {
			isValidStopLoss = true
		}

		if isValidStopLoss {
			log.Printf("  → Restoring stop-loss for remaining position %.4f: %.2f", remainingQuantity, decision.NewStopLoss)
			err = at.trader.SetStopLoss(decision.Symbol, positionSide, remainingQuantity, decision.NewStopLoss)
			if err != nil {
				log.Printf("  ⚠️ Failed to restore stop-loss: %v (doesn't affect close result)", err)
			}
		} else {
			priceGapPct := math.Abs((decision.NewStopLoss-marketData.CurrentPrice)/marketData.CurrentPrice) * 100
			log.Printf("  ⚠️⚠️ 跳过设置止损：价格不合理 (止损 %.2f, 当前 %.2f, 差距 %.2f%%)",
				decision.NewStopLoss, marketData.CurrentPrice, priceGapPct)
			log.Printf("  → %s仓位的止损必须%s当前价，剩余仓位目前没有止损保护",
				positionSide, map[string]string{"LONG": "低于", "SHORT": "高于"}[positionSide])
		}
	}

	if decision.NewTakeProfit > 0 {
		// ⚡ 验证止盈价格合理性（防止 code=-2021 错误）
		isValidTakeProfit := false
		if positionSide == "LONG" && decision.NewTakeProfit > marketData.CurrentPrice {
			isValidTakeProfit = true
		} else if positionSide == "SHORT" && decision.NewTakeProfit < marketData.CurrentPrice {
			isValidTakeProfit = true
		}

		if isValidTakeProfit {
			log.Printf("  → Restoring take-profit for remaining position %.4f: %.2f", remainingQuantity, decision.NewTakeProfit)
			err = at.trader.SetTakeProfit(decision.Symbol, positionSide, remainingQuantity, decision.NewTakeProfit)
			if err != nil {
				log.Printf("  ⚠️ Failed to restore take-profit: %v (doesn't affect close result)", err)
			}
		} else {
			priceGapPct := math.Abs((decision.NewTakeProfit-marketData.CurrentPrice)/marketData.CurrentPrice) * 100
			log.Printf("  ⚠️⚠️ 跳过设置止盈：价格不合理 (止盈 %.2f, 当前 %.2f, 差距 %.2f%%)",
				decision.NewTakeProfit, marketData.CurrentPrice, priceGapPct)
			log.Printf("  → %s仓位的止盈必须%s当前价，剩余仓位目前没有止盈保护",
				positionSide, map[string]string{"LONG": "高于", "SHORT": "低于"}[positionSide])
		}
	}

	// 如果 AI 没有提供新的止盈止损，记录警告
	if decision.NewStopLoss <= 0 && decision.NewTakeProfit <= 0 {
		log.Printf("  ⚠️⚠️⚠️ 警告: 部分平仓后AI未提供新的止盈止损价格")
		log.Printf("  → 剩余仓位 %.4f (价值 %.2f USDT) 目前没有止盈止损保护", remainingQuantity, remainingValue)
		log.Printf("  → 建议: 在 partial_close 决策中包含 new_stop_loss 和 new_take_profit 字段")
	}

	return nil
}

// GetID 获取trader ID
func (at *AutoTrader) GetID() string {
	return at.id
}

// GetName 获取trader名称
func (at *AutoTrader) GetName() string {
	return at.name
}

// GetAIModel 获取AI模型
func (at *AutoTrader) GetAIModel() string {
	return at.aiModel
}

// GetExchange 获取交易所
func (at *AutoTrader) GetExchange() string {
	return at.exchange
}

// SetCustomPrompt 设置自定义交易策略prompt
func (at *AutoTrader) SetCustomPrompt(prompt string) {
	at.customPrompt = prompt
}

// SetOverrideBasePrompt 设置是否覆盖基础prompt
func (at *AutoTrader) SetOverrideBasePrompt(override bool) {
	at.overrideBasePrompt = override
}

// SetSystemPromptTemplate 设置系统提示词模板
func (at *AutoTrader) SetSystemPromptTemplate(templateName string) {
	at.systemPromptTemplate = templateName
}

// GetSystemPromptTemplate 获取当前系统提示词模板名称
func (at *AutoTrader) GetSystemPromptTemplate() string {
	return at.systemPromptTemplate
}

// GetDecisionLogger 获取决策日志记录器
func (at *AutoTrader) GetDecisionLogger() logger.IDecisionLogger {
	return at.decisionLogger
}

// GetStatus 获取系统状态（用于API）
func (at *AutoTrader) GetStatus() map[string]interface{} {
	aiProvider := "DeepSeek"
	if at.config.UseQwen {
		aiProvider = "Qwen"
	}

	return map[string]interface{}{
		"trader_id":       at.id,
		"trader_name":     at.name,
		"ai_model":        at.aiModel,
		"exchange":        at.exchange,
		"is_running":      at.isRunning,
		"start_time":      at.startTime.Format(time.RFC3339),
		"runtime_minutes": int(time.Since(at.startTime).Minutes()),
		"call_count":      at.callCount,
		"initial_balance": at.initialBalance,
		"scan_interval":   at.config.ScanInterval.String(),
		"stop_until":      at.stopUntil.Format(time.RFC3339),
		"last_reset_time": at.lastResetTime.Format(time.RFC3339),
		"ai_provider":     aiProvider,
	}
}

// GetAccountInfo 获取账户信息（用于API）
func (at *AutoTrader) GetAccountInfo() (map[string]interface{}, error) {
	balance, err := at.trader.GetBalance()
	if err != nil {
		return nil, fmt.Errorf("获取余额失败: %w", err)
	}

	// 获取账户字段
	totalWalletBalance := 0.0
	totalUnrealizedProfit := 0.0
	availableBalance := 0.0

	if wallet, ok := balance["totalWalletBalance"].(float64); ok {
		totalWalletBalance = wallet
	}
	if unrealized, ok := balance["totalUnrealizedProfit"].(float64); ok {
		totalUnrealizedProfit = unrealized
	}
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}

	// Total Equity = 钱包余额 + 未实现盈亏
	totalEquity := totalWalletBalance + totalUnrealizedProfit

	// 获取持仓计算总保证金
	positions, err := at.trader.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("获取持仓失败: %w", err)
	}

	totalMarginUsed := 0.0
	totalUnrealizedPnLCalculated := 0.0
	for _, pos := range positions {
		entryPrice := pos["entryPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity
		}
		unrealizedPnl := pos["unRealizedProfit"].(float64)
		totalUnrealizedPnLCalculated += unrealizedPnl

		leverage := 10
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev)
		}
		marginUsed := (quantity * entryPrice) / float64(leverage)
		totalMarginUsed += marginUsed
	}

	// 验证未实现盈亏的一致性（API值 vs 从持仓计算）
	diff := math.Abs(totalUnrealizedProfit - totalUnrealizedPnLCalculated)
	if diff > 0.1 { // 允许0.01 USDT的误差
		log.Printf("⚠️ 未实现盈亏不一致: API=%.4f, 计算=%.4f, 差异=%.4f",
			totalUnrealizedProfit, totalUnrealizedPnLCalculated, diff)
	}

	totalPnL := totalEquity - at.initialBalance
	totalPnLPct := 0.0
	if at.initialBalance > 0 {
		totalPnLPct = (totalPnL / at.initialBalance) * 100
	} else {
		log.Printf("⚠️ Initial Balance异常: %.2f，无法计算PNL百分比", at.initialBalance)
	}

	marginUsedPct := 0.0
	if totalEquity > 0 {
		marginUsedPct = (totalMarginUsed / totalEquity) * 100
	}

	return map[string]interface{}{
		// 核心字段
		"total_equity":      totalEquity,           // 账户净值 = wallet + unrealized
		"wallet_balance":    totalWalletBalance,    // 钱包余额（不含未实现盈亏）
		"unrealized_profit": totalUnrealizedProfit, // 未实现盈亏（交易所API官方值）
		"available_balance": availableBalance,      // 可用余额

		// 盈亏统计
		"total_pnl":       totalPnL,          // 总盈亏 = equity - initial
		"total_pnl_pct":   totalPnLPct,       // 总盈亏百分比
		"initial_balance": at.initialBalance, // 初始余额
		"daily_pnl":       at.dailyPnL,       // 日盈亏

		// 持仓信息
		"position_count":  len(positions),  // 持仓数量
		"margin_used":     totalMarginUsed, // 保证金占用
		"margin_used_pct": marginUsedPct,   // 保证金使用率
	}, nil
}

func sameDay(a, b time.Time) bool {
	ay, am, ad := a.Date()
	by, bm, bd := b.Date()
	return ay == by && am == bm && ad == bd
}

// GetPositions 获取持仓列表（用于API）
func (at *AutoTrader) GetPositions() ([]map[string]interface{}, error) {
	positions, err := at.trader.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("获取持仓失败: %w", err)
	}

	var result []map[string]interface{}
	for _, pos := range positions {
		symbol := pos["symbol"].(string)
		side := pos["side"].(string)
		entryPrice := pos["entryPrice"].(float64)
		markPrice := pos["markPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity
		}
		unrealizedPnl := pos["unRealizedProfit"].(float64)
		liquidationPrice := pos["liquidationPrice"].(float64)

		leverage := 10
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev)
		}

		// 计算占用保证金（基于开仓价，而非当前价）
		marginUsed := (quantity * entryPrice) / float64(leverage)

		// 计算盈亏百分比（基于保证金）
		pnlPct := calculatePnLPercentage(unrealizedPnl, marginUsed)

		result = append(result, map[string]interface{}{
			"symbol":             symbol,
			"side":               side,
			"entry_price":        entryPrice,
			"mark_price":         markPrice,
			"quantity":           quantity,
			"leverage":           leverage,
			"unrealized_pnl":     unrealizedPnl,
			"unrealized_pnl_pct": pnlPct,
			"liquidation_price":  liquidationPrice,
			"margin_used":        marginUsed,
		})
	}

	return result, nil
}

// calculatePnLPercentage 计算盈亏百分比（基于保证金，自动考虑杠杆）
// 收益率 = 未实现盈亏 / 保证金 × 100%
func calculatePnLPercentage(unrealizedPnl, marginUsed float64) float64 {
	if marginUsed > 0 {
		return (unrealizedPnl / marginUsed) * 100
	}
	return 0.0
}

// sortDecisionsByPriority 对决策排序：先平仓，再开仓，最后hold/wait
// 这样可以避免换仓时仓位叠加超限
func sortDecisionsByPriority(decisions []decision.Decision) []decision.Decision {
	if len(decisions) <= 1 {
		return decisions
	}

	// 定义优先级
	getActionPriority := func(action string) int {
		switch action {
		case "close_long", "close_short", "partial_close":
			return 1 // 最高优先级：先平仓（包括部分平仓）
		case "update_stop_loss", "update_take_profit":
			return 2 // 调整持仓止盈止损
		case "open_long", "open_short":
			return 3 // 次优先级：后开仓
		case "hold", "wait":
			return 4 // 最低优先级：观望
		default:
			return 999 // 未知动作放最后
		}
	}

	// 复制决策列表
	sorted := make([]decision.Decision, len(decisions))
	copy(sorted, decisions)

	// 按优先级排序
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if getActionPriority(sorted[i].Action) > getActionPriority(sorted[j].Action) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted
}

// getCandidateCoins 获取交易员的候选币种列表
func (at *AutoTrader) getCandidateCoins() ([]decision.CandidateCoin, error) {
	// 优先级 1: 自定义币种列表（最高优先级）
	if len(at.tradingCoins) > 0 {
		var candidateCoins []decision.CandidateCoin
		for _, coin := range at.tradingCoins {
			symbol := normalizeSymbol(coin)
			candidateCoins = append(candidateCoins, decision.CandidateCoin{
				Symbol:  symbol,
				Sources: []string{"custom"},
			})
		}
		log.Printf("📋 [%s] 使用自定义币种: %d个币种 %v",
			at.name, len(candidateCoins), at.tradingCoins)
		return candidateCoins, nil
	}

	// 优先级 2: 信号源扩展模式（合并系统默认 + 信号源）
	if at.useCoinPool || at.useOITop {
		symbolMap := make(map[string][]string) // symbol -> sources
		coinPoolURL := strings.TrimSpace(at.coinPoolAPIURL)
		oiTopURL := strings.TrimSpace(at.oiTopAPIURL)

		// 2.1 先添加系统默认币种作为基础
		defaultCount := 0
		for _, coin := range at.defaultCoins {
			symbol := normalizeSymbol(coin)
			symbolMap[symbol] = []string{"default"}
			defaultCount++
		}

		// 2.2 根据配置添加信号源币种（扩展候选范围）
		const ai500Limit = 20
		signalSourceCount := 0

		if at.useCoinPool && at.useOITop {
			// 同时使用 AI500 + OI Top
			mergedPool, err := pool.GetMergedCoinPoolWithOverride(ai500Limit, coinPoolURL, oiTopURL)
			if err == nil {
				for _, symbol := range mergedPool.AllSymbols {
					sources := mergedPool.SymbolSources[symbol]
					if existingSources, exists := symbolMap[symbol]; exists {
						// 币种已存在（来自默认），合并来源标签
						symbolMap[symbol] = append(existingSources, sources...)
					} else {
						// 新币种（来自信号源）
						symbolMap[symbol] = sources
						signalSourceCount++
					}
				}
			} else if err != nil {
				log.Printf("⚠️  [%s] 获取合并信号源失败: %v", at.name, err)
			}
		} else if at.useCoinPool {
			// 只使用 AI500
			var ai500Pool []string
			var err error
			if coinPoolURL != "" {
				ai500Pool, err = pool.GetTopRatedCoinsWithURL(ai500Limit, coinPoolURL)
			} else {
				ai500Pool, err = pool.GetTopRatedCoins(ai500Limit)
			}
			if err == nil {
				for _, symbol := range ai500Pool {
					if existingSources, exists := symbolMap[symbol]; exists {
						symbolMap[symbol] = append(existingSources, "ai500")
					} else {
						symbolMap[symbol] = []string{"ai500"}
						signalSourceCount++
					}
				}
			} else if err != nil {
				log.Printf("⚠️  [%s] 获取 AI500 信号失败: %v", at.name, err)
			}
		} else if at.useOITop {
			// 只使用 OI Top
			var oiTopPool []pool.OIPosition
			var err error
			if oiTopURL != "" {
				oiTopPool, err = pool.GetOITopPositionsWithURL(oiTopURL)
			} else {
				oiTopPool, err = pool.GetOITopPositions()
			}
			if err == nil {
				limit := 20
				if len(oiTopPool) < limit {
					limit = len(oiTopPool)
				}
				for i := 0; i < limit; i++ {
					symbol := oiTopPool[i].Symbol
					if existingSources, exists := symbolMap[symbol]; exists {
						symbolMap[symbol] = append(existingSources, "oi_top")
					} else {
						symbolMap[symbol] = []string{"oi_top"}
						signalSourceCount++
					}
				}
			} else if err != nil {
				log.Printf("⚠️  [%s] 获取 OI Top 信号失败: %v", at.name, err)
			}
		}

		// 2.3 构建候选币种列表
		var candidateCoins []decision.CandidateCoin
		for symbol, sources := range symbolMap {
			candidateCoins = append(candidateCoins, decision.CandidateCoin{
				Symbol:  symbol,
				Sources: sources,
			})
		}

		log.Printf("📋 [%s] 信号源扩展模式: 系统默认%d + 信号源新增%d = 总计%d个候选币种",
			at.name, defaultCount, signalSourceCount, len(candidateCoins))
		return candidateCoins, nil
	}

	// 优先级 3: 只使用系统默认币种（未启用信号源）
	if len(at.defaultCoins) > 0 {
		var candidateCoins []decision.CandidateCoin
		for _, coin := range at.defaultCoins {
			symbol := normalizeSymbol(coin)
			candidateCoins = append(candidateCoins, decision.CandidateCoin{
				Symbol:  symbol,
				Sources: []string{"default"},
			})
		}
		log.Printf("📋 [%s] 使用系统默认币种: %d个币种 %v",
			at.name, len(candidateCoins), at.defaultCoins)
		return candidateCoins, nil
	}

	// 优先级 4: 都没有配置 - 返回空列表（AI 只管理现有持仓）
	log.Printf("⚠️  [%s] 无任何币种来源，AI 将只管理现有持仓（不开新仓）", at.name)
	return []decision.CandidateCoin{}, nil
}

// normalizeSymbol 标准化币种符号（确保以USDT结尾）
func normalizeSymbol(symbol string) string {
	// 转为大写
	symbol = strings.ToUpper(strings.TrimSpace(symbol))

	// 确保以USDT结尾
	if !strings.HasSuffix(symbol, "USDT") {
		symbol = symbol + "USDT"
	}

	return symbol
}

// 启动回撤监控
func (at *AutoTrader) startDrawdownMonitor() {
	at.monitorWg.Add(1)
	go func() {
		defer at.monitorWg.Done()

		ticker := time.NewTicker(1 * time.Minute) // 每分钟检查一次
		defer ticker.Stop()

		log.Println("📊 启动持仓回撤监控（每分钟检查一次）")

		for {
			select {
			case <-ticker.C:
				at.checkPositionDrawdown()
			case <-at.stopMonitorCh:
				log.Println("⏹ 停止持仓回撤监控")
				return
			}
		}
	}()
}

// 检查持仓回撤情况
func (at *AutoTrader) checkPositionDrawdown() {
	// 获取当前持仓
	positions, err := at.trader.GetPositions()
	if err != nil {
		log.Printf("❌ 回撤监控：获取持仓失败: %v", err)
		return
	}

	for _, pos := range positions {
		symbol := pos["symbol"].(string)
		side := pos["side"].(string)
		entryPrice := pos["entryPrice"].(float64)
		markPrice := pos["markPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity // 空仓数量为负，转为正数
		}

		// 计算当前盈亏百分比
		leverage := 10 // 默认值
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev)
		}

		var currentPnLPct float64
		if side == "long" {
			currentPnLPct = ((markPrice - entryPrice) / entryPrice) * float64(leverage) * 100
		} else {
			currentPnLPct = ((entryPrice - markPrice) / entryPrice) * float64(leverage) * 100
		}

		// 构造持仓唯一标识（区分多空）
		posKey := symbol + "_" + side

		// 获取该持仓的历史最高收益
		at.peakPnLCacheMutex.RLock()
		peakPnLPct, exists := at.peakPnLCache[posKey]
		at.peakPnLCacheMutex.RUnlock()

		if !exists {
			// 如果没有历史最高记录，使用当前盈亏作为初始值
			peakPnLPct = currentPnLPct
			at.UpdatePeakPnL(symbol, side, currentPnLPct)
		} else {
			// 更新峰值缓存
			at.UpdatePeakPnL(symbol, side, currentPnLPct)
		}

		// 计算回撤（从最高点下跌的幅度）
		var drawdownPct float64
		if peakPnLPct > 0 && currentPnLPct < peakPnLPct {
			drawdownPct = ((peakPnLPct - currentPnLPct) / peakPnLPct) * 100
		}

		// 检查平仓条件：收益大于5%且回撤超过40%
		if currentPnLPct > 5.0 && drawdownPct >= 40.0 {
			log.Printf("🚨 触发回撤平仓条件: %s %s | 当前收益: %.2f%% | 最高收益: %.2f%% | 回撤: %.2f%%",
				symbol, side, currentPnLPct, peakPnLPct, drawdownPct)

			// 执行平仓
			if err := at.emergencyClosePosition(symbol, side); err != nil {
				log.Printf("❌ 回撤平仓失败 (%s %s): %v", symbol, side, err)
			} else {
				log.Printf("✅ 回撤平仓成功: %s %s", symbol, side)
				// 平仓后清理该持仓的缓存
				at.ClearPeakPnLCache(symbol, side)
			}
		} else if currentPnLPct > 5.0 {
			// 记录接近平仓条件的情况（用于调试）
			log.Printf("📊 回撤监控: %s %s | 收益: %.2f%% | 最高: %.2f%% | 回撤: %.2f%%",
				symbol, side, currentPnLPct, peakPnLPct, drawdownPct)
		}
	}
}

// 紧急平仓函数
func (at *AutoTrader) emergencyClosePosition(symbol, side string) error {
	switch side {
	case "long":
		order, err := at.trader.CloseLong(symbol, 0) // 0 = 全部平仓
		if err != nil {
			return err
		}
		log.Printf("✅ 紧急平多仓成功，订单ID: %v", order["orderId"])
	case "short":
		order, err := at.trader.CloseShort(symbol, 0) // 0 = 全部平仓
		if err != nil {
			return err
		}
		log.Printf("✅ 紧急平空仓成功，订单ID: %v", order["orderId"])
	default:
		return fmt.Errorf("未知的持仓方向: %s", side)
	}

	return nil
}

// GetPeakPnLCache 获取最高收益缓存
func (at *AutoTrader) GetPeakPnLCache() map[string]float64 {
	at.peakPnLCacheMutex.RLock()
	defer at.peakPnLCacheMutex.RUnlock()

	// 返回缓存的副本
	cache := make(map[string]float64)
	for k, v := range at.peakPnLCache {
		cache[k] = v
	}
	return cache
}

// UpdatePeakPnL 更新最高收益缓存
func (at *AutoTrader) UpdatePeakPnL(symbol, side string, currentPnLPct float64) {
	at.peakPnLCacheMutex.Lock()
	defer at.peakPnLCacheMutex.Unlock()

	posKey := symbol + "_" + side
	if peak, exists := at.peakPnLCache[posKey]; exists {
		// 更新峰值（如果是多头，取较大值；如果是空头，currentPnLPct为负，也要比较）
		if currentPnLPct > peak {
			at.peakPnLCache[posKey] = currentPnLPct
		}
	} else {
		// 首次记录
		at.peakPnLCache[posKey] = currentPnLPct
	}
}

// ClearPeakPnLCache 清除指定持仓的峰值缓存
func (at *AutoTrader) ClearPeakPnLCache(symbol, side string) {
	at.peakPnLCacheMutex.Lock()
	defer at.peakPnLCacheMutex.Unlock()

	posKey := symbol + "_" + side
	delete(at.peakPnLCache, posKey)
}

// detectClosedPositions 检测被交易所自动平仓的持仓（止损/止盈触发）
// 对比上一次和当前的持仓快照，找出消失的持仓
func (at *AutoTrader) detectClosedPositions(currentPositions []decision.PositionInfo) []decision.PositionInfo {
	// 首次运行或没有缓存，返回空列表
	if at.lastPositions == nil || len(at.lastPositions) == 0 {
		return []decision.PositionInfo{}
	}

	// 构建当前持仓的 key 集合
	currentKeys := make(map[string]bool)
	for _, pos := range currentPositions {
		key := pos.Symbol + "_" + pos.Side
		currentKeys[key] = true
	}

	// 检测消失的持仓
	var closedPositions []decision.PositionInfo
	for key, lastPos := range at.lastPositions {
		if !currentKeys[key] {
			// 持仓消失了，说明被自动平仓（止损/止盈触发）
			closedPositions = append(closedPositions, lastPos)
		}
	}

	return closedPositions
}

// generateAutoCloseActions 为被动平仓的持仓生成 DecisionAction
// generateAutoCloseActions - Create DecisionActions for passive closes with intelligent price/reason inference
func (at *AutoTrader) generateAutoCloseActions(closedPositions []decision.PositionInfo) []logger.DecisionAction {
	var actions []logger.DecisionAction

	for _, pos := range closedPositions {
		// 确定动作类型
		action := "auto_close_long"
		if pos.Side == "short" {
			action = "auto_close_short"
		}

		// 智能推断平仓价格和原因
		closePrice, closeReason := at.inferCloseDetails(pos)

		// 生成 DecisionAction
		actions = append(actions, logger.DecisionAction{
			Action:    action,
			Symbol:    pos.Symbol,
			Quantity:  pos.Quantity,
			Leverage:  pos.Leverage,
			Price:     closePrice, // 推断的平仓价格（止损/止盈/强平/市价）
			OrderID:   0,          // 自动平仓没有订单ID
			Timestamp: time.Now(), // 检测时间（非真实触发时间）
			Success:   true,
			Error:     closeReason, // 使用 Error 字段存储平仓原因（stop_loss/take_profit/liquidation/manual/unknown）
		})
	}

	return actions
}

// inferCloseDetails - Intelligently infer close price and reason based on position data
func (at *AutoTrader) inferCloseDetails(pos decision.PositionInfo) (price float64, reason string) {
	const priceThreshold = 0.01 // 1% 价格阈值，用于判断是否接近目标价格

	markPrice := pos.MarkPrice

	// 1. 优先检查是否接近强平价（爆仓）- 因为这是最严重的情况
	if pos.LiquidationPrice > 0 {
		liquidationThreshold := 0.02 // 2% 强平价阈值（更宽松，因为接近强平时会被系统平仓）
		if pos.Side == "long" {
			// 多头爆仓：价格接近强平价
			if markPrice <= pos.LiquidationPrice*(1+liquidationThreshold) {
				return pos.LiquidationPrice, "liquidation"
			}
		} else {
			// 空头爆仓：价格接近强平价
			if markPrice >= pos.LiquidationPrice*(1-liquidationThreshold) {
				return pos.LiquidationPrice, "liquidation"
			}
		}
	}

	// 2. 检查是否触发止损
	if pos.StopLoss > 0 {
		if pos.Side == "long" {
			// 多头止损：价格跌破止损价
			if markPrice <= pos.StopLoss*(1+priceThreshold) {
				return pos.StopLoss, "stop_loss"
			}
		} else {
			// 空头止损：价格涨破止损价
			if markPrice >= pos.StopLoss*(1-priceThreshold) {
				return pos.StopLoss, "stop_loss"
			}
		}
	}

	// 3. 检查是否触发止盈
	if pos.TakeProfit > 0 {
		if pos.Side == "long" {
			// 多头止盈：价格涨到止盈价
			if markPrice >= pos.TakeProfit*(1-priceThreshold) {
				return pos.TakeProfit, "take_profit"
			}
		} else {
			// 空头止盈：价格跌到止盈价
			if markPrice <= pos.TakeProfit*(1+priceThreshold) {
				return pos.TakeProfit, "take_profit"
			}
		}
	}

	// 4. 无法判断原因，可能是手动平仓或其他原因
	// 使用当前市场价作为估算平仓价
	return markPrice, "unknown"
}

// updatePositionSnapshot 更新持仓快照（在每次 buildTradingContext 后调用）
func (at *AutoTrader) updatePositionSnapshot(currentPositions []decision.PositionInfo) {
	// 清空旧快照
	at.lastPositions = make(map[string]decision.PositionInfo)

	// 保存当前持仓快照
	for _, pos := range currentPositions {
		key := pos.Symbol + "_" + pos.Side
		at.lastPositions[key] = pos
	}
}

// ReloadAIModelConfig 重新加载AI模型配置（热更新）
// 这个方法允许在运行时更新AI模型配置，无需重启trader
func (at *AutoTrader) ReloadAIModelConfig(modelConfig *config.AIModelConfig) error {
	if modelConfig == nil {
		return fmt.Errorf("模型配置为空")
	}

	log.Printf("🔄 [%s] 重新加载AI模型配置...", at.name)

	// 更新AI模型相关配置
	at.config.CustomModelName = modelConfig.CustomModelName
	at.config.CustomAPIURL = modelConfig.CustomAPIURL

	// 根据不同的AI provider更新对应的API Key
	switch modelConfig.Provider {
	case "deepseek":
		at.config.DeepSeekKey = modelConfig.APIKey
		at.config.CustomAPIKey = modelConfig.APIKey
		log.Printf("✓ [%s] DeepSeek配置已更新: Model=%s, BaseURL=%s",
			at.name, at.config.CustomModelName, at.config.CustomAPIURL)
	case "qwen":
		at.config.QwenKey = modelConfig.APIKey
		log.Printf("✓ [%s] Qwen配置已更新: Model=%s",
			at.name, at.config.CustomModelName)
	case "custom":
		at.config.CustomAPIKey = modelConfig.APIKey
		log.Printf("✓ [%s] 自定义AI配置已更新: URL=%s, Model=%s",
			at.name, at.config.CustomAPIURL, at.config.CustomModelName)
	default:
		return fmt.Errorf("不支持的AI provider: %s", modelConfig.Provider)
	}

	// 重新初始化MCP客户端以应用新配置
	if err := at.reinitializeMCPClient(); err != nil {
		return fmt.Errorf("重新初始化MCP客户端失败: %w", err)
	}

	log.Printf("✅ [%s] AI模型配置热更新完成", at.name)
	return nil
}

// reinitializeMCPClient 重新初始化MCP客户端
func (at *AutoTrader) reinitializeMCPClient() error {
	// 根据当前配置确定使用的 API Key
	var apiKey string
	switch at.config.AIModel {
	case "qwen":
		apiKey = at.config.QwenKey
	case "deepseek":
		apiKey = at.config.DeepSeekKey
	case "custom":
		apiKey = at.config.CustomAPIKey
	default:
		// 如果有自定义配置，使用自定义 key
		if at.config.CustomAPIKey != "" {
			apiKey = at.config.CustomAPIKey
		} else if at.config.DeepSeekKey != "" {
			apiKey = at.config.DeepSeekKey
		} else {
			apiKey = at.config.QwenKey
		}
	}

	// 使用统一的 SetAPIKey 方法重新初始化
	at.mcpClient.SetAPIKey(apiKey, at.config.CustomAPIURL, at.config.CustomModelName)

	log.Printf("🔧 [MCP] AI模型配置已重新初始化: Model=%s, Provider=%s, CustomURL=%s",
		at.config.CustomModelName, at.config.AIModel, at.config.CustomAPIURL)

	return nil
}

func enforceScanInterval(interval time.Duration, traderName string) time.Duration {
	minInterval := 2 * time.Minute
	maxInterval := 3 * time.Minute

	if interval <= 0 {
		log.Printf("⚙️ [%s] 未配置扫描间隔，默认使用 3m", traderName)
		return 3 * time.Minute
	}

	if interval < minInterval {
		log.Printf("⚠️ [%s] 扫描间隔 %.2f 分钟过快，自动调整为 2m", traderName, interval.Minutes())
		return minInterval
	}

	if interval > maxInterval {
		log.Printf("⚠️ [%s] 扫描间隔 %.2f 分钟过慢，自动调整为 3m", traderName, interval.Minutes())
		return maxInterval
	}

	return interval
}

func ensureTimeframes(configured []string, traderName string) []string {
	defaultTF := []string{"3m", "5m", "15m", "1h", "4h"}

	if len(configured) == 0 {
		log.Printf("⚙️ [%s] 未配置时间线，默认使用 %v", traderName, defaultTF)
		return append([]string{}, defaultTF...)
	}

	seen := make(map[string]bool)
	normalized := make([]string, 0, len(configured))

	for _, tf := range configured {
		tf = strings.ToLower(strings.TrimSpace(tf))
		if tf == "" || seen[tf] {
			continue
		}
		seen[tf] = true
		normalized = append(normalized, tf)
	}

	if len(normalized) == 0 {
		log.Printf("⚙️ [%s] 时间线列表为空，默认使用 %v", traderName, defaultTF)
		return append([]string{}, defaultTF...)
	}

	if !seen["1h"] {
		insertIdx := len(normalized)
		for idx, tf := range normalized {
			if tf == "4h" || tf == "6h" || tf == "12h" || tf == "1d" {
				insertIdx = idx
				break
			}
		}
		normalized = insertString(normalized, insertIdx, "1h")
		log.Printf("⚙️ [%s] 时间线缺少 1h，已自动补齐: %v", traderName, normalized)
	}

	return normalized
}

func insertString(values []string, index int, value string) []string {
	if index < 0 || index > len(values) {
		index = len(values)
	}
	values = append(values, "")
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}

var checklistLinePattern = regexp.MustCompile(`^((\d+|[①②③④⑤⑥⑦⑧⑨])[\.\)、\)]|[一二三四五六七八九]+\s*[、\.)])`)

func countChecklistItems(reasoning string) int {
	lines := strings.Split(reasoning, "\n")
	count := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if checklistLinePattern.MatchString(line) ||
			strings.HasPrefix(line, "- ") ||
			strings.HasPrefix(line, "* ") ||
			strings.HasPrefix(line, "•") ||
			strings.HasPrefix(line, "✔") ||
			strings.HasPrefix(line, "✅") {
			count++
		}
	}
	return count
}

func (at *AutoTrader) enforceReasoningOrWait(decision *decision.Decision) (bool, string) {
	if decision == nil {
		return false, ""
	}

	if decision.Action != "open_long" && decision.Action != "open_short" {
		return false, ""
	}

	reasoning := strings.TrimSpace(decision.Reasoning)
	upper := strings.ToUpper(reasoning)
	lower := strings.ToLower(reasoning)
	missing := []string{}

	if reasoning == "" || (!strings.Contains(reasoning, "状态") && !strings.Contains(lower, "state")) {
		missing = append(missing, "状态识别")
	}

	checklistCount := countChecklistItems(reasoning)
	if checklistCount < 7 {
		missing = append(missing, fmt.Sprintf("入场验证 %d/7", checklistCount))
	}

	if !strings.Contains(upper, "RR") && !strings.Contains(reasoning, "风险回报") {
		missing = append(missing, "RR 计算")
	}

	if len(missing) == 0 {
		return false, ""
	}

	originalAction := decision.Action
	summary := fmt.Sprintf("⚠️ %s %s 缺少关键信息: %s → 自动 wait",
		decision.Symbol, originalAction, strings.Join(missing, "、"))
	decision.Action = "wait"
	decision.Reasoning = fmt.Sprintf("Skipped due to missing sections: %s", strings.Join(missing, "、"))
	log.Println(summary)
	return true, summary
}

func calculateRiskUSD(quantity, entryPrice, stopLoss float64, side string) float64 {
	if quantity <= 0 || entryPrice <= 0 || stopLoss <= 0 {
		return 0
	}

	var riskPerUnit float64
	switch strings.ToUpper(side) {
	case "LONG":
		riskPerUnit = entryPrice - stopLoss
	case "SHORT":
		riskPerUnit = stopLoss - entryPrice
	default:
		return 0
	}

	if riskPerUnit <= 0 {
		return 0
	}

	return riskPerUnit * quantity
}

func (at *AutoTrader) estimateAccountEquity(balance map[string]interface{}, fallback float64) float64 {
	wallet, _ := balance["totalWalletBalance"].(float64)
	unrealized, _ := balance["totalUnrealizedProfit"].(float64)
	equity := wallet + unrealized

	if equity <= 0 {
		if avail, ok := balance["availableBalance"].(float64); ok && avail > 0 {
			equity = avail
		}
	}

	if equity <= 0 {
		if fallback > 0 {
			equity = fallback
		} else {
			equity = at.initialBalance
		}
	}

	return equity
}

func (at *AutoTrader) annotateRisk(decision *decision.Decision, actionRecord *logger.DecisionAction, side string, quantity, entryPrice, stopLoss, equity float64) {
	riskUSD := calculateRiskUSD(quantity, entryPrice, stopLoss, side)
	if riskUSD <= 0 {
		return
	}

	originalRisk := decision.RiskUSD
	decision.RiskUSD = riskUSD
	actionRecord.RiskUSD = riskUSD
	if equity > 0 {
		actionRecord.RiskPct = (riskUSD / equity) * 100
	}

	message := fmt.Sprintf("  📏 实际风险: %.2f USDT (%.2f%% / Equity %.2f)", riskUSD, actionRecord.RiskPct, equity)
	if originalRisk > 0 && math.Abs(originalRisk-riskUSD) > 0.01 {
		message += fmt.Sprintf(" [AI请求 %.2f]", originalRisk)
	}
	log.Println(message)

	if actionRecord.RiskPct > 4 {
		log.Printf("  ⚠️ 风险占比 %.2f%% 超过 4%%，建议 AI 调整仓位或止损", actionRecord.RiskPct)
	}
}
