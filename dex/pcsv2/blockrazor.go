package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type BlockRazorBundle struct {
	ChainID          string                       `json:"chainID"`
	Hash             string                       `json:"hash"`
	Transactions     []Tx                         `json:"txs"`
	NextBlockNumber  uint64                       `json:"nextBlockNumber"`
	MaxBlockNumber   uint64                       `json:"maxBlockNumber"`
	ProxyBidContract string                       `json:"proxyBidContract"`
	RefundAddress    string                       `json:"refundAddress"`
	RefundCfg        int                          `json:"refundCfg"`
	State            map[string]map[string]string `json:"state,omitempty"`
}

type Tx struct {
	Hash     string          `json:"hash"`
	From     string          `json:"from"`
	To       string          `json:"to"`
	Value    string          `json:"value"`
	Nonce    uint64          `json:"nonce"`
	Calldata string          `json:"calldata"`
	Method   string          `json:"functionSelector"`
	Logs     []BlockRazorLog `json:"logs"`
}

type BlockRazorLog struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

func (bot *MEVBot) startBlockRazorSubscription(ctx context.Context) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := bot.connectBlockRazor(ctx, bot.config.ScutumURL); err != nil {
					bot.logger.Error("BlockRazor connection error: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}
			}
		}
	}()

	return nil
}

func (bot *MEVBot) connectBlockRazor(ctx context.Context, endpoint string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	reader := bufio.NewReader(resp.Body)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("failed to read line: %v", err)
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")
				var bundle BlockRazorBundle
				if err := json.Unmarshal([]byte(data), &bundle); err != nil {
					bot.logger.Error("Failed to unmarshal BlockRazor bundle: %v", err)
					continue
				}
				for _, tx := range bundle.Transactions {
					if !strings.EqualFold(tx.To, PancakeRouterV2.String()) {
						continue
					}

					calldataBz, err := hex.DecodeString(strings.TrimPrefix(tx.Calldata, "0x"))
					if err != nil {
						bot.logger.Error("Failed to decode calldata: %v, tx_hash: %s", err, tx.Hash)
						continue
					}

					methodName, args, err := bot.decodePancakeSwapCalldata(tx.Method, calldataBz)
					if err != nil {
						continue
					}

					swapInfo := &SwapInfo{
						DEX:      "PancakeSwap",
						Function: methodName,
					}
					if err := bot.populateBlockRazorSwapInfo(swapInfo, tx, methodName, args); err != nil {
						bot.logger.Error("Failed to populate swap info: %v", err)
						continue
					}

					if swapInfo.TokenIn == (common.Address{}) || swapInfo.TokenOut == (common.Address{}) {
						bot.logger.Warning("Missing token addresses in swap info for tx %s", tx.Hash)
						continue
					}

					if swapInfo.AmountIn == nil || swapInfo.AmountOutMin == nil {
						bot.logger.Warning("Missing amount information in swap info for tx %s", tx.Hash)
						continue
					}
					go bot.handleSwap(swapInfo, tx, bundle)
				}
			}
		}
	}
}

func (bot *MEVBot) populateBlockRazorSwapInfo(swapInfo *SwapInfo, tx Tx, methodName string, args []interface{}) error {
	switch methodName {
	case "swapExactETHForTokens":
		if len(args) < 4 {
			return fmt.Errorf("insufficient arguments for %s", methodName)
		}

		amountOutMin, ok := args[0].(*big.Int)
		if !ok {
			return fmt.Errorf("failed to parse amountOutMin")
		}

		path, ok := args[1].([]common.Address)
		if !ok {
			return fmt.Errorf("failed to parse path")
		}

		to, ok := args[2].(common.Address)
		if !ok {
			return fmt.Errorf("failed to parse to address")
		}

		deadline, ok := args[3].(*big.Int)
		if !ok {
			return fmt.Errorf("failed to parse deadline")
		}

		valueInt := new(big.Int)
		if _, ok := valueInt.SetString(strings.TrimPrefix(tx.Value, "0x"), 16); !ok {
			if _, ok := valueInt.SetString(tx.Value, 10); !ok {
				return fmt.Errorf("failed to parse value: %s", tx.Value)
			}
		}

		swapInfo.AmountIn = valueInt
		swapInfo.AmountOutMin = amountOutMin
		swapInfo.Path = path
		swapInfo.Recipient = to
		swapInfo.Deadline = deadline

		if len(path) > 0 {
			swapInfo.TokenIn = path[0]
			swapInfo.TokenOut = path[len(path)-1]
		}

	case "swapExactTokensForETH", "swapExactTokensForTokens":
		if len(args) < 5 {
			return fmt.Errorf("insufficient arguments for %s", methodName)
		}

		amountIn, ok := args[0].(*big.Int)
		if !ok {
			return fmt.Errorf("failed to parse amountIn")
		}

		amountOutMin, ok := args[1].(*big.Int)
		if !ok {
			return fmt.Errorf("failed to parse amountOutMin")
		}

		path, ok := args[2].([]common.Address)
		if !ok {
			return fmt.Errorf("failed to parse path")
		}

		to, ok := args[3].(common.Address)
		if !ok {
			return fmt.Errorf("failed to parse to address")
		}

		deadline, ok := args[4].(*big.Int)
		if !ok {
			return fmt.Errorf("failed to parse deadline")
		}

		swapInfo.AmountIn = amountIn
		swapInfo.AmountOutMin = amountOutMin
		swapInfo.Path = path
		swapInfo.Recipient = to
		swapInfo.Deadline = deadline

		if len(path) > 0 {
			swapInfo.TokenIn = path[0]
			swapInfo.TokenOut = path[len(path)-1]
		}

	case "swapETHForExactTokens":
		// swapETHForExactTokens(uint256 amountOut, address[] calldata path, address to, uint256 deadline)
		if len(args) < 4 {
			return fmt.Errorf("insufficient arguments for %s", methodName)
		}

		amountOut, ok := args[0].(*big.Int)
		if !ok {
			return fmt.Errorf("failed to parse amountOut")
		}

		path, ok := args[1].([]common.Address)
		if !ok {
			return fmt.Errorf("failed to parse path")
		}

		to, ok := args[2].(common.Address)
		if !ok {
			return fmt.Errorf("failed to parse to address")
		}

		deadline, ok := args[3].(*big.Int)
		if !ok {
			return fmt.Errorf("failed to parse deadline")
		}

		valueInt := new(big.Int)
		if _, ok := valueInt.SetString(strings.TrimPrefix(tx.Value, "0x"), 16); !ok {
			if _, ok := valueInt.SetString(tx.Value, 10); !ok {
				return fmt.Errorf("failed to parse value: %s", tx.Value)
			}
		}

		swapInfo.AmountIn = valueInt
		swapInfo.AmountOutMin = amountOut
		swapInfo.Path = path
		swapInfo.Recipient = to
		swapInfo.Deadline = deadline

		// 设置代币地址
		if len(path) > 0 {
			swapInfo.TokenIn = path[0]
			swapInfo.TokenOut = path[len(path)-1]
		}

	case "swapTokensForExactETH", "swapTokensForExactTokens":
		if len(args) < 5 {
			return fmt.Errorf("insufficient arguments for %s", methodName)
		}

		amountOut, ok := args[0].(*big.Int)
		if !ok {
			return fmt.Errorf("failed to parse amountOut")
		}

		amountInMax, ok := args[1].(*big.Int)
		if !ok {
			return fmt.Errorf("failed to parse amountInMax")
		}

		path, ok := args[2].([]common.Address)
		if !ok {
			return fmt.Errorf("failed to parse path")
		}

		to, ok := args[3].(common.Address)
		if !ok {
			return fmt.Errorf("failed to parse to address")
		}

		deadline, ok := args[4].(*big.Int)
		if !ok {
			return fmt.Errorf("failed to parse deadline")
		}

		swapInfo.AmountIn = amountInMax
		swapInfo.AmountOutMin = amountOut
		swapInfo.Path = path
		swapInfo.Recipient = to
		swapInfo.Deadline = deadline

		if len(path) > 0 {
			swapInfo.TokenIn = path[0]
			swapInfo.TokenOut = path[len(path)-1]
		}

	default:
		return fmt.Errorf("unsupported method: %s", methodName)
	}

	return nil
}

func (bot *MEVBot) handleSwap(swapInfo *SwapInfo, tx Tx, bundle BlockRazorBundle) {

	//// todo: strategy 1 should match exact pair config(A_B)
	pairFound := false
	for _, pair := range bot.config.MonitorPairs {
		if (swapInfo.TokenIn == pair.Token1 && swapInfo.TokenOut == pair.Token2) ||
			(swapInfo.TokenIn == pair.Token2 && swapInfo.TokenOut == pair.Token1) {
			pairFound = true
			break
		}
	}

	if !pairFound {
		return
	}

	// todo strategy 2 should match only one

	if !bot.isTokenSafe(swapInfo.TokenOut) {
		bot.logger.Warning("Potentially unsafe token detected: %s, skipping", swapInfo.TokenOut.Hex())
		return
	}

	bot.logger.Info("Decoded %s swap via PancakeSwap: %s -> %s",
		swapInfo.Function, swapInfo.AmountIn.String(), swapInfo.AmountOutMin.String())

	bot.logger.Info("Starting to find optimal backrun amount for function: %s, txHash: %s at %s",
		swapInfo.Function, tx.Hash, time.Now().Format("2006-01-02 15:04:05.000"))

	// todo: test code
	//var backrunInput *big.Int
	//var err error
	//
	profit := big.NewInt(0)
	//
	//// backrun a USDT - > BNB swap
	//swapInfo.Function = "swapTokensForExactTokens"
	//swapInfo.TokenIn = common.HexToAddress("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")  // wbnb
	//swapInfo.TokenOut = common.HexToAddress("0x55d398326f99059ff775485246999027b3197955") // USDT
	//swapInfo.Path[0] = common.HexToAddress("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
	//swapInfo.Path[1] = common.HexToAddress("0x55d398326f99059ff775485246999027b3197955")
	//
	backrunInput := big.NewInt(10000000000000000) // 0.01 UST

	// todo below part is calculating the optimal backrun amount, and the profit
	//backrunInput, profit, err := bot.findOptimalBackrunAmount(swapInfo)
	//if err != nil {
	//	bot.logger.Warning("Error calculating optimal backrun: %v", err)
	//	return
	//}
	//bot.logger.Info("Found profitable backrun opportunity for tx %s. backRun input %s, Expected profit: %s BNB at %s",
	//	tx.Hash, backrunInput.String(), formatEthValue(profit), time.Now().Format("2006-01-02 15:04:05.000"))

	err := bot.createAndSubmitBackrunBundleForBlockRazor(swapInfo, tx, bundle, backrunInput, profit)
	if err != nil {
		bot.logger.Error("Failed to create/submit backrun bundle for tx %s: %v", tx.Hash, err)
		return
	}

	bot.logger.Info("Successfully submitted backrun bundle for tx %s at time %s",
		tx.Hash, time.Now().Format(time.RFC3339))
}

// createAndSubmitBackrunBundleForBlockRazor 为 BlockRazor 交易创建和提交 backrun bundle
func (bot *MEVBot) createAndSubmitBackrunBundleForBlockRazor(swapInfo *SwapInfo, tx Tx, bundle BlockRazorBundle, backrunInput *big.Int, expectedProfit *big.Int) error {
	var err error
	bot.logger.Info("Creating backrun bundle for tx %s with expected profit: %s BNB",
		tx.Hash, formatEthValue(expectedProfit))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reversedPath := make([]common.Address, len(swapInfo.Path))
	for i, addr := range swapInfo.Path {
		reversedPath[len(swapInfo.Path)-1-i] = addr
	}

	if !bot.config.SkipApprovalCheck {
	}

	backrunTx, err := bot.createBlockRazorBackrunTransaction(swapInfo, backrunInput, reversedPath,
		common.HexToAddress(bundle.ProxyBidContract),
		common.HexToAddress(bundle.RefundAddress),
		uint64(bundle.RefundCfg),
		big.NewInt(10000000000000), // 0.00001 BNB bid value to the proxy bid contract
	)

	if err != nil {
		return fmt.Errorf("failed to create backrun transaction: %v", err)
	}
	bot.logger.Info("the backrun transaction hash: %s", backrunTx.Hash().Hex())

	if bot.config.SimulateTransactions {
		err = bot.simulateTransaction(ctx, bot.address, backrunTx)
		if err != nil {
			return fmt.Errorf("transaction simulation failed: %v", err)
		}
		bot.logger.Info("Backrun transaction simulation successful")
	}

	backrunTxRaw, err := bot.getRawTransaction(backrunTx)
	if err != nil {
		return fmt.Errorf("failed to get raw backrun transaction: %v", err)
	}

	bundleRequest := BackrunBundle{
		Hash:              bundle.Hash,
		Txs:               []string{backrunTxRaw},
		RevertingTxHashes: []string{},
		MaxBlockNumber:    uint64(bot.blockNum.Load() + 2),
		Hint: &Hint{
			Hash:             true,
			From:             false,
			To:               true,
			Value:            false,
			Nonce:            false,
			Calldata:         true,
			FunctionSelector: true,
			Logs:             true,
		},
		RefundAddress: bot.config.RefundAddress.Hex(),
	}

	//bot.logger.Info("calling backrun bundle for original bundle hash: %s, tx hash: %s at time: %s",
	//	bundle.Hash, tx.Hash, time.Now().Format("2006-01-02 15:04:05.000"))
	//
	//err = bot.callBundle(bundleRequest, backrunTx.Nonce()+1, true)
	//if err != nil {
	//	bot.logger.Error("Failed to call backrun bundle: %v", err)
	//	return fmt.Errorf("failed to call bundle: %v", err)
	//}
	//
	//bot.logger.Info("call bundle success, bundle hash: %s", bundle.Hash)

	bundleHash, err := bot.submitBundle(bundleRequest, false, backrunTx.Nonce()+1, true)
	if err != nil {
		return fmt.Errorf("failed to submit bundle: %v", err)
	}

	bot.logger.Info("Successfully submitted bundle with hash: %s", bundleHash)
	return nil
}
