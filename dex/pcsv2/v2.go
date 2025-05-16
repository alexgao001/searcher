package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/joho/godotenv"
)

// LogLevel represents the logging level
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
)

// Constants for the application
const (
	PancakeRouterABI = `[{"inputs":[{"internalType":"address","name":"_factory","type":"address"},{"internalType":"address","name":"_WETH","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"WETH","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"amountADesired","type":"uint256"},{"internalType":"uint256","name":"amountBDesired","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"addLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"},{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountTokenDesired","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"addLiquidityETH","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"},{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"reserveIn","type":"uint256"},{"internalType":"uint256","name":"reserveOut","type":"uint256"}],"name":"getAmountIn","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"reserveIn","type":"uint256"},{"internalType":"uint256","name":"reserveOut","type":"uint256"}],"name":"getAmountOut","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsIn","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsOut","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"reserveA","type":"uint256"},{"internalType":"uint256","name":"reserveB","type":"uint256"}],"name":"quote","outputs":[{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidityETH","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidityETHSupportingFeeOnTransferTokens","outputs":[{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityETHWithPermit","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityETHWithPermitSupportingFeeOnTransferTokens","outputs":[{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityWithPermit","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapETHForExactTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactETHForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactETHForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForETH","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForETHSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapTokensForExactETH","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapTokensForExactTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"}]`
	ERC20ABI         = `[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"},{"name":"_spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]`
	BackrunABI       = `[
  {
    "type": "constructor",
    "inputs": [],
    "stateMutability": "nonpayable"
  },
  {
    "type": "receive",
    "stateMutability": "payable"
  },
  {
    "type": "function",
    "name": "executeBackrunETHToToken",
    "inputs": [
      {
        "name": "router",
        "type": "address",
        "internalType": "address"
      },
      {
        "name": "amountOutMin",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "path",
        "type": "address[]",
        "internalType": "address[]"
      },
      {
        "name": "deadline",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "proxyBidContract",
        "type": "address",
        "internalType": "address"
      },
      {
        "name": "refundAddress",
        "type": "address",
        "internalType": "address"
      },
      {
        "name": "refundCfg",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "bidValue",
        "type": "uint256",
        "internalType": "uint256"
      }
    ],
    "outputs": [],
    "stateMutability": "payable"
  },
  {
    "type": "function",
    "name": "executeBackrunTokenToETH",
    "inputs": [
      {
        "name": "router",
        "type": "address",
        "internalType": "address"
      },
      {
        "name": "amountIn",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "amountOutMin",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "path",
        "type": "address[]",
        "internalType": "address[]"
      },
      {
        "name": "deadline",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "proxyBidContract",
        "type": "address",
        "internalType": "address"
      },
      {
        "name": "refundAddress",
        "type": "address",
        "internalType": "address"
      },
      {
        "name": "refundCfg",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "bidValue",
        "type": "uint256",
        "internalType": "uint256"
      }
    ],
    "outputs": [],
    "stateMutability": "nonpayable"
  },
  {
    "type": "function",
    "name": "executeBackrunTokenToToken",
    "inputs": [
      {
        "name": "router",
        "type": "address",
        "internalType": "address"
      },
      {
        "name": "amountIn",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "amountOutMin",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "path",
        "type": "address[]",
        "internalType": "address[]"
      },
      {
        "name": "deadline",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "proxyBidContract",
        "type": "address",
        "internalType": "address"
      },
      {
        "name": "refundAddress",
        "type": "address",
        "internalType": "address"
      },
      {
        "name": "refundCfg",
        "type": "uint256",
        "internalType": "uint256"
      },
      {
        "name": "bidValue",
        "type": "uint256",
        "internalType": "uint256"
      }
    ],
    "outputs": [],
    "stateMutability": "payable"
  },
  {
    "type": "function",
    "name": "owner",
    "inputs": [],
    "outputs": [
      {
        "name": "",
        "type": "address",
        "internalType": "address"
      }
    ],
    "stateMutability": "view"
  },
  {
    "type": "function",
    "name": "setOwner",
    "inputs": [
      {
        "name": "newOwner",
        "type": "address",
        "internalType": "address"
      }
    ],
    "outputs": [],
    "stateMutability": "nonpayable"
  },
  {
    "type": "function",
    "name": "withdrawETH",
    "inputs": [],
    "outputs": [],
    "stateMutability": "nonpayable"
  },
  {
    "type": "function",
    "name": "withdrawToken",
    "inputs": [
      {
        "name": "token",
        "type": "address",
        "internalType": "address"
      }
    ],
    "outputs": [],
    "stateMutability": "nonpayable"
  }
]`
)

// PancakeSwap addresses and method signatures
var (
	// Native token (BNB on BSC)
	WBNB = common.HexToAddress("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")

	// PancakeSwap Router addresses (BSC)
	PancakeRouterV2 = common.HexToAddress("0x10ED43C718714eb63d5aA57B78B54704E256024E")

	// PancakeSwap Factory address (BSC)
	PancakeFactoryV2 = common.HexToAddress("0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73")

	// PancakeSwap method selectors
	SwapExactETHForTokens    = "0x7ff36ab5" // swapExactETHForTokens(uint256,address[],address,uint256)
	SwapExactTokensForETH    = "0x18cbafe5" // swapExactTokensForETH(uint256,uint256,address[],address,uint256)
	SwapExactTokensForTokens = "0x38ed1739" // swapExactTokensForTokens(uint256,uint256,address[],address,uint256)
	SwapETHForExactTokens    = "0xfb3bdb41" // swapETHForExactTokens(uint256,address[],address,uint256)
	SwapTokensForExactETH    = "0x4a25d94a" // swapTokensForExactETH(uint256,uint256,address[],address,uint256)
	SwapTokensForExactTokens = "0x8803dbee" // swapTokensForExactTokens(uint256,uint256,address[],address,uint256)

	// Stablecoins
	stablecoins = []common.Address{
		common.HexToAddress("0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56"), // BUSD
		common.HexToAddress("0x55d398326f99059fF775485246999027B3197955"), // USDT
		common.HexToAddress("0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"), // USDC
	}
)

// Config holds all configuration parameters
type Config struct {
	BuildersRpcURLs string

	NodeURL string
	WSURL   string

	ScutumURL string

	PrivateKey                   string
	ChainID                      int64
	MinProfitThreshold           *big.Int
	RouterAddresses              map[string]bool
	RefundAddress                common.Address
	PollInterval                 time.Duration
	LogLevel                     LogLevel
	MaxGasPrice                  *big.Int
	GasPriceMultiplier           int64
	GasLimit                     uint64
	SimulateTransactions         bool
	WaitForApprovalConfirmations uint64
	MonitorPairs                 []TokenPair
	SkipApprovalCheck            bool
	MaxGasTip                    *big.Int
	GasTipMultiplier             int64
	BuildersEoaAddress           []common.Address
	BackrunContractAddress       common.Address
}

type TokenPair struct {
	Token1 common.Address
	Token2 common.Address
}

// SwapInfo contains decoded swap details
type SwapInfo struct {
	DEX          string
	Function     string
	TargetTx     *types.Transaction
	TokenIn      common.Address
	TokenOut     common.Address
	AmountIn     *big.Int
	AmountOutMin *big.Int
	Path         []common.Address
	Deadline     *big.Int
	Recipient    common.Address
	TxHash       string
}

// BackrunBundle represents the bundle to be submitted to BlockRazor
type BackrunBundle struct {
	Hash              string   `json:"hash"`
	Txs               []string `json:"txs"`
	RevertingTxHashes []string `json:"revertingTxHashes"`
	MaxBlockNumber    uint64   `json:"maxBlockNumber"`
	Hint              *Hint    `json:"hint,omitempty"`
	RefundAddress     string   `json:"refundAddress,omitempty"`
}

// Hint controls the disclosure of transaction fields
type Hint struct {
	Hash             bool `json:"hash,omitempty"`
	From             bool `json:"from,omitempty"`
	To               bool `json:"to,omitempty"`
	Value            bool `json:"value,omitempty"`
	Nonce            bool `json:"nonce,omitempty"`
	Calldata         bool `json:"calldata,omitempty"`
	FunctionSelector bool `json:"functionSelector,omitempty"`
	Logs             bool `json:"logs,omitempty"`
}

// JsonRpcRequest represents a JSON-RPC request
type JsonRpcRequest struct {
	JsonRpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

// JsonRpcResponse represents a JSON-RPC response
type JsonRpcResponse struct {
	JsonRpc string      `json:"jsonrpc"`
	Result  interface{} `json:"result"`
	Error   interface{} `json:"error,omitempty"`
	ID      int         `json:"id"`
}

// TxPoolContent represents the content of the transaction pool
type TxPoolContent struct {
	Pending map[string]map[string]*RPCTransaction `json:"pending"`
	Queued  map[string]map[string]*RPCTransaction `json:"queued"`
}

// RPCTransaction represents a transaction in the RPC response
type RPCTransaction struct {
	From     string `json:"from"`
	Gas      string `json:"gas"`
	GasPrice string `json:"gasPrice"`
	Hash     string `json:"hash"`
	Input    string `json:"input"`
	Nonce    string `json:"nonce"`
	To       string `json:"to"`
	Value    string `json:"value"`
	R        string `json:"r"`
	S        string `json:"s"`
	V        string `json:"v"`
	Type     string `json:"type"`
}

// PendingTransaction represents a transaction in the pending transaction pool
type PendingTransaction struct {
	From     string `json:"from"`
	Gas      string `json:"gas"`
	GasPrice string `json:"gasPrice"`
	Hash     string `json:"hash"`
	Input    string `json:"input"`
	Nonce    string `json:"nonce"`
	To       string `json:"to"`
	Value    string `json:"value"`
	R        string `json:"r"`
	S        string `json:"s"`
	V        string `json:"v"`
	Type     string `json:"type"`
}

// Logger provides custom logging with levels
type Logger struct {
	level LogLevel
}

// MEVBot encapsulates the bot's functionality
type MEVBot struct {
	config     Config
	client     *ethclient.Client
	privateKey *ecdsa.PrivateKey
	address    common.Address
	logger     *Logger
	blockNum   *atomic.Int64

	seenTxs   map[string]bool
	seenTxsMu sync.Mutex

	nonce    *atomic.Int64
	gasPrice *atomic.Int64
	gasTip   *atomic.Int64
}

// NewLogger creates a new logger with specified log level
func NewLogger(level LogLevel) *Logger {
	return &Logger{level: level}
}

// Log methods for different levels
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level <= DEBUG {
		log.Printf("[DEBUG] "+format, args...)
	}
}

func (l *Logger) Info(format string, args ...interface{}) {
	if l.level <= INFO {
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		log.Printf("[INFO] [%s] "+format, append([]interface{}{timestamp}, args...)...)
	}
}

func (l *Logger) Warning(format string, args ...interface{}) {
	if l.level <= WARNING {
		log.Printf("[WARNING] "+format, args...)
	}
}

func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= ERROR {
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		log.Printf("[ERROR] [%s] "+format, append([]interface{}{timestamp}, args...)...)
	}
}

// NewMEVBot creates a new MEV bot instance
func NewMEVBot(config Config) (*MEVBot, error) {
	logger := NewLogger(config.LogLevel)

	// Connect to Ethereum client
	client, err := ethclient.Dial(config.NodeURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node: %v", err)
	}

	// Verify ChainID matches the node
	nodeChainID, err := client.ChainID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get ChainID from node: %v", err)
	}
	if nodeChainID.Int64() != config.ChainID {
		return nil, fmt.Errorf("chainID mismatch: config has %d, node has %d", config.ChainID, nodeChainID.Int64())
	}

	// Setup private key
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(config.PrivateKey, "0x"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %v", err)
	}

	// Derive address from private key
	address := crypto.PubkeyToAddress(*privateKey.Public().(*ecdsa.PublicKey))

	var nonce = new(atomic.Int64)
	chainNonce, err := client.NonceAt(context.Background(), address, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get nonce: %v", err)
	}
	nonce.Store(int64(chainNonce))

	var gasPrice = new(atomic.Int64)
	chainGasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get gas price: %v", err)
	}
	gasPrice.Store(chainGasPrice.Int64())

	var gasTip = new(atomic.Int64)
	chainGasTip, err := client.SuggestGasTipCap(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get gas tip: %v", err)
	}
	gasTip.Store(chainGasTip.Int64())

	return &MEVBot{
		config:     config,
		client:     client,
		privateKey: privateKey,
		address:    address,
		logger:     logger,
		blockNum:   new(atomic.Int64),
		seenTxs:    make(map[string]bool),
		nonce:      nonce,
		gasPrice:   gasPrice,
		gasTip:     gasTip,
	}, nil
}

// Start initializes and starts the MEV bot
func (bot *MEVBot) Start() error {
	// Check wallet balance
	balance, err := bot.client.BalanceAt(context.Background(), bot.address, nil)
	if err != nil {
		return fmt.Errorf("failed to get wallet balance: %v", err)
	}
	bot.logger.Info("Using wallet address: %s", bot.address.Hex())
	bot.logger.Info("Wallet balance: %s BNB", formatEthValue(balance))

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start block number tracking
	go bot.trackBlockNumber(ctx)

	// Start tx pool monitoring
	go bot.StartTxMonitor(ctx)

	// Start cleanup routine for seen transactions
	go bot.cleanupSeenTxs(ctx)

	go bot.startNonceAndGasPriceSync(ctx)

	// Handle interrupts for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	bot.logger.Info("Shutting down gracefully...")
	cancel()
	time.Sleep(time.Second) // Give goroutines time to exit

	return nil
}

func (bot *MEVBot) StartTxMonitor(ctx context.Context) {
	if os.Getenv("ENABLE_SCUTUM") == "true" {
		err := bot.startBlockRazorSubscription(ctx)
		if err != nil {
			return
		}
	} else {
		bot.monitorTxPool(ctx)
	}
}

func (bot *MEVBot) startNonceAndGasPriceSync(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nonce, err := bot.client.NonceAt(context.Background(), bot.address, nil)
			if err != nil {
				bot.logger.Warning("Error getting nonce: %v", err)
				continue
			}
			bot.nonce.Store(int64(nonce))
			bot.logger.Debug("Current nonce: %d", nonce)

			gasPrice, err := bot.client.SuggestGasPrice(ctx)
			if err != nil {
				bot.logger.Warning("Error getting gas price: %v", err)
				continue
			}

			// Apply gas price multiplier
			gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(bot.config.GasPriceMultiplier))
			gasPrice = new(big.Int).Div(gasPrice, big.NewInt(100))

			// Respect max gas price
			if gasPrice.Cmp(bot.config.MaxGasPrice) > 0 {
				gasPrice = new(big.Int).Set(bot.config.MaxGasPrice)
			}
			bot.gasPrice.Store(gasPrice.Int64())

			// Update gas tip
			gasTip, err := bot.client.SuggestGasTipCap(ctx)
			if err != nil {
				bot.logger.Warning("Error getting gas tip: %v", err)
				continue
			}

			// Apply gas tip multiplier
			gasTip = new(big.Int).Mul(gasTip, big.NewInt(bot.config.GasTipMultiplier))
			gasTip = new(big.Int).Div(gasTip, big.NewInt(100))

			// Respect max gas tip
			if gasTip.Cmp(bot.config.MaxGasTip) > 0 {
				gasTip = new(big.Int).Set(bot.config.MaxGasTip)
			}
			bot.gasTip.Store(gasTip.Int64())
		}
	}
}

// trackBlockNumber periodically updates the current block number
func (bot *MEVBot) trackBlockNumber(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			blockNum, err := bot.client.BlockNumber(context.Background())
			if err != nil {
				bot.logger.Warning("Error getting block number: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			bot.blockNum.Store(int64(blockNum))
			bot.logger.Debug("Current block number: %d", blockNum)
			time.Sleep(1 * time.Second)
		}
	}
}

// cleanupSeenTxs periodically cleans up the seen transactions map
func (bot *MEVBot) cleanupSeenTxs(ctx context.Context) {
	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			bot.seenTxsMu.Lock()
			bot.logger.Debug("Cleaning up seen transactions map, current size: %d", len(bot.seenTxs))
			// If we have too many transactions, reset the map
			if len(bot.seenTxs) > 10000 {
				bot.seenTxs = make(map[string]bool)
				bot.logger.Info("Reset seen transactions map (exceeded 10000 entries)")
			}
			bot.seenTxsMu.Unlock()
		}
	}
}

// monitorTxPool polls the transaction pool for new transactions
func (bot *MEVBot) monitorTxPool(ctx context.Context) {
	bot.logger.Info("Started transaction pool monitoring")
	wsClient, err := rpc.Dial(bot.config.WSURL)
	if err != nil {
		bot.logger.Error("Failed to connect to WebSocket: %v", err)
		return
	}

	client := ethclient.NewClient(wsClient)
	defer client.Close()
	pendingTxs := make(chan common.Hash)
	sub, err := wsClient.EthSubscribe(ctx, pendingTxs, "newPendingTransactions")
	if err != nil {
		bot.logger.Error("Failed to subscribe to pending transactions: %v", err)
		return
	}
	defer sub.Unsubscribe()

	bot.logger.Info("Successfully subscribed to pending transactions")

	for {
		select {
		case <-ctx.Done():
			bot.logger.Info("Transaction pool monitoring stopped due to context cancellation")
			return
		case err := <-sub.Err():
			bot.logger.Error("Subscription error: %v", err)
			return
		case txHash := <-pendingTxs:
			go func(hash common.Hash) {
				getTxTime := time.Now()

				tx, isPending, err := client.TransactionByHash(ctx, hash)
				if err != nil {
					return
				}

				if !isPending {
					return
				}
				txHashStr := hash.Hex()
				bot.seenTxsMu.Lock()
				if bot.seenTxs[txHashStr] {
					bot.seenTxsMu.Unlock()
					return
				}
				bot.seenTxs[txHashStr] = true
				bot.seenTxsMu.Unlock()
				if tx.To() == nil {
					return
				}

				chainID := tx.ChainId()
				var from common.Address

				if chainID == nil {
					chainID = big.NewInt(56)
				}

				switch tx.Type() {
				case 0:
					signer := types.NewEIP155Signer(chainID)
					from, err = types.Sender(signer, tx)
				case 1:
					signer := types.NewEIP2930Signer(chainID)
					from, err = types.Sender(signer, tx)
				case 2:
					signer := types.NewLondonSigner(chainID)
					from, err = types.Sender(signer, tx)
				default:
					signer := types.LatestSignerForChainID(chainID)
					from, err = types.Sender(signer, tx)
				}

				if err != nil {
					bot.logger.Error("Error getting transaction sender: %v, tx type: %d", err, tx.Type())
					return
				}

				if !bot.config.RouterAddresses[strings.ToLower(tx.To().Hex())] {
					return
				}

				data := tx.Data()
				if len(data) < 10 {
					return
				}
				functionSelector := hexutil.Encode(data[:4])
				if !isPancakeSwapFunction(functionSelector) {
					return
				}

				bot.logger.Info("Found potential PancakeSwap transaction: %s from %s at %s",
					txHashStr, from.Hex(), getTxTime.Format("2006-01-02 15:04:05.000"))

				bot.processPendingTx(tx)
			}(txHash)
		}
	}
}

// getTransactionByHash gets a transaction by its hash
func (bot *MEVBot) getTransactionByHash(txHash string) (*types.Transaction, bool, error) {
	hash := common.HexToHash(txHash)
	return bot.client.TransactionByHash(context.Background(), hash)
}

// processPendingTx is the main entry point for processing a pending transaction
func (bot *MEVBot) processPendingTx(tx *types.Transaction) {
	txHash := tx.Hash().Hex()

	// Try to decode the swap transaction
	swapInfo, err := bot.decodePancakeSwap(tx)
	if err != nil {
		bot.logger.Warning("Failed to decode swap for transaction %s: %v", txHash, err)
		return
	}

	if swapInfo == nil {
		bot.logger.Warning("No swap info found for transaction %s", txHash)
		return
	}

	// Check if tokenIn and tokenOut are both being monitored
	if swapInfo.TokenIn == (common.Address{}) || swapInfo.TokenOut == (common.Address{}) {
		bot.logger.Warning("TokenIn or TokenOut is empty for transaction %s", txHash)
		return
	}

	// Check if this pair is being monitored
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

	if !bot.isTokenSafe(swapInfo.TokenOut) {
		bot.logger.Warning("Potentially unsafe token detected: %s, skipping", swapInfo.TokenOut.Hex())
		return
	}

	bot.logger.Info("Decoded %s swap via PancakeSwap: %s -> %s",
		swapInfo.Function, swapInfo.AmountIn.String(), swapInfo.AmountOutMin.String())

	bot.logger.Info("Starting to find optimal backrun amount for function: %s, txHash: %s at %s",
		swapInfo.Function, swapInfo.TargetTx.Hash().Hex(), time.Now().Format("2006-01-02 15:04:05.000"))

	// Hardcode the backrun input and profit amounts based on token types
	var backrunInput *big.Int
	var profit = big.NewInt(10000000000000000) // 0.01 BNB

	if swapInfo.TokenIn == common.HexToAddress("0x55d398326f99059ff775485246999027b3197955") {
		backrunInput = big.NewInt(10000000000000000) // For USDT
	} else {
		backrunInput = big.NewInt(500000000000000000) // 0.005 BNB
	}

	bot.logger.Info("Found profitable backrun opportunity for tx %s. Expected profit: %s BNB at %s",
		txHash, formatEthValue(profit), time.Now().Format("2006-01-02 15:04:05.000"))

	// Create and submit backrun bundle
	err = bot.createAndSubmitBackrunBundle(swapInfo, backrunInput, profit)
	if err != nil {
		bot.logger.Error("Failed to create/submit backrun bundle for tx %s: %v", txHash, err)
		return
	}

	bot.logger.Info("Successfully submitted backrun bundle for tx %s at time %s",
		txHash, time.Now().Format(time.RFC3339))
}

// decodePancakeSwap decodes a PancakeSwap transaction
func (bot *MEVBot) decodePancakeSwap(tx *types.Transaction) (*SwapInfo, error) {
	// Check if transaction is to PancakeSwap router
	if tx.To() == nil || *tx.To() != PancakeRouterV2 {
		return nil, fmt.Errorf("transaction not sent to PancakeSwap router")
	}

	// Get function selector from input data
	data := tx.Data()
	if len(data) < 4 {
		return nil, fmt.Errorf("transaction data too short")
	}
	selector := hexutil.Encode(data[:4])

	// Decode the calldata
	methodName, args, err := bot.decodePancakeSwapCalldata(selector, data)
	if err != nil {
		return nil, err
	}

	// Create SwapInfo struct
	swapInfo := &SwapInfo{
		DEX:      "PancakeSwap",
		Function: methodName,
		TargetTx: tx,
		TxHash:   tx.Hash().Hex(),
	}

	// Parse arguments based on function type
	switch methodName {
	case "swapExactETHForTokens":
		// swapExactETHForTokens(uint256 amountOutMin, address[] calldata path, address to, uint256 deadline)
		if len(args) < 4 {
			return nil, fmt.Errorf("insufficient arguments for %s", methodName)
		}

		amountOutMin, ok := args[0].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("failed to parse amountOutMin")
		}

		path, ok := args[1].([]common.Address)
		if !ok {
			return nil, fmt.Errorf("failed to parse path")
		}

		to, ok := args[2].(common.Address)
		if !ok {
			return nil, fmt.Errorf("failed to parse to address")
		}

		deadline, ok := args[3].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("failed to parse deadline")
		}

		// For swapExactETHForTokens, input amount is the transaction value
		swapInfo.AmountIn = tx.Value()
		swapInfo.AmountOutMin = amountOutMin
		swapInfo.Path = path
		swapInfo.Recipient = to
		swapInfo.Deadline = deadline

		// Set token addresses
		if len(path) > 0 {
			swapInfo.TokenIn = path[0] // Should be WBNB
			swapInfo.TokenOut = path[len(path)-1]
		}

	case "swapExactTokensForETH", "swapExactTokensForTokens":
		// Both functions have signature: (uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline)
		if len(args) < 5 {
			return nil, fmt.Errorf("insufficient arguments for %s", methodName)
		}

		amountIn, ok := args[0].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("failed to parse amountIn")
		}

		amountOutMin, ok := args[1].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("failed to parse amountOutMin")
		}

		path, ok := args[2].([]common.Address)
		if !ok {
			return nil, fmt.Errorf("failed to parse path")
		}

		to, ok := args[3].(common.Address)
		if !ok {
			return nil, fmt.Errorf("failed to parse to address")
		}

		deadline, ok := args[4].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("failed to parse deadline")
		}

		swapInfo.AmountIn = amountIn
		swapInfo.AmountOutMin = amountOutMin
		swapInfo.Path = path
		swapInfo.Recipient = to
		swapInfo.Deadline = deadline

		// Set token addresses
		if len(path) > 0 {
			swapInfo.TokenIn = path[0]
			swapInfo.TokenOut = path[len(path)-1]
		}

	case "swapETHForExactTokens":
		// swapETHForExactTokens(uint256 amountOut, address[] calldata path, address to, uint256 deadline)
		if len(args) < 4 {
			return nil, fmt.Errorf("insufficient arguments for %s", methodName)
		}

		amountOut, ok := args[0].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("failed to parse amountOut")
		}

		path, ok := args[1].([]common.Address)
		if !ok {
			return nil, fmt.Errorf("failed to parse path")
		}

		to, ok := args[2].(common.Address)
		if !ok {
			return nil, fmt.Errorf("failed to parse to address")
		}

		deadline, ok := args[3].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("failed to parse deadline")
		}

		// For swapETHForExactTokens, input is the transaction value and amountOutMin is the expected output
		swapInfo.AmountIn = tx.Value()
		swapInfo.AmountOutMin = amountOut // This is actually the exact output amount
		swapInfo.Path = path
		swapInfo.Recipient = to
		swapInfo.Deadline = deadline

		// Set token addresses
		if len(path) > 0 {
			swapInfo.TokenIn = path[0] // Should be WBNB
			swapInfo.TokenOut = path[len(path)-1]
		}

	case "swapTokensForExactETH", "swapTokensForExactTokens":
		// Both functions have signature: (uint256 amountOut, uint256 amountInMax, address[] path, address to, uint256 deadline)
		if len(args) < 5 {
			return nil, fmt.Errorf("insufficient arguments for %s", methodName)
		}

		amountOut, ok := args[0].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("failed to parse amountOut")
		}

		amountInMax, ok := args[1].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("failed to parse amountInMax")
		}

		path, ok := args[2].([]common.Address)
		if !ok {
			return nil, fmt.Errorf("failed to parse path")
		}

		to, ok := args[3].(common.Address)
		if !ok {
			return nil, fmt.Errorf("failed to parse to address")
		}

		deadline, ok := args[4].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("failed to parse deadline")
		}

		// For swapTokensForExact* functions, amountIn is the maximum input and amountOutMin is the exact output
		swapInfo.AmountIn = amountInMax
		swapInfo.AmountOutMin = amountOut
		swapInfo.Path = path
		swapInfo.Recipient = to
		swapInfo.Deadline = deadline

		// Set token addresses
		if len(path) > 0 {
			swapInfo.TokenIn = path[0]
			swapInfo.TokenOut = path[len(path)-1]
		}
	}

	// Validate path
	if len(swapInfo.Path) < 2 {
		return nil, fmt.Errorf("invalid path length: %d", len(swapInfo.Path))
	}

	return swapInfo, nil
}

// decodePancakeSwapCalldata decodes the calldata of a PancakeSwap transaction
func (bot *MEVBot) decodePancakeSwapCalldata(selector string, data []byte) (string, []interface{}, error) {
	// Find method by selector
	var methodName string

	switch selector {
	case SwapExactETHForTokens:
		methodName = "swapExactETHForTokens"
	case SwapExactTokensForETH:
		methodName = "swapExactTokensForETH"
	case SwapExactTokensForTokens:
		methodName = "swapExactTokensForTokens"
	case SwapETHForExactTokens:
		methodName = "swapETHForExactTokens"
	case SwapTokensForExactETH:
		methodName = "swapTokensForExactETH"
	case SwapTokensForExactTokens:
		methodName = "swapTokensForExactTokens"
	default:
		return "", nil, fmt.Errorf("unsupported function selector: %s", selector)
	}

	// Load PancakeSwap Router ABI
	routerABI, err := abi.JSON(strings.NewReader(PancakeRouterABI))
	if err != nil {
		return "", nil, fmt.Errorf("failed to load PancakeSwap ABI: %v", err)
	}

	method, found := routerABI.Methods[methodName]
	if !found {
		return "", nil, fmt.Errorf("method %s not found in ABI", methodName)
	}

	// Try to decode the transaction input
	args, err := method.Inputs.Unpack(data[4:])
	if err != nil {
		return "", nil, fmt.Errorf("failed to decode function inputs: %v", err)
	}

	return methodName, args, nil
}

// createAndSubmitBackrunBundle creates and submits a backrun bundle
func (bot *MEVBot) createAndSubmitBackrunBundle(swapInfo *SwapInfo, backrunInput *big.Int, expectedProfit *big.Int) error {
	var err error
	bot.logger.Info("Creating backrun bundle for tx %s with expected profit: %s BNB",
		swapInfo.TargetTx.Hash().Hex(), formatEthValue(expectedProfit))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Reverse the path for backrun
	reversedPath := make([]common.Address, len(swapInfo.Path))
	for i, addr := range swapInfo.Path {
		reversedPath[len(swapInfo.Path)-1-i] = addr
	}
	// skip the approval check
	if !bot.config.SkipApprovalCheck {
		// Check if token approval is needed and get approval tx if necessary
		var approvalTx *types.Transaction

		if swapInfo.Function == "swapExactETHForTokens" || swapInfo.Function == "swapETHForExactTokens" ||
			swapInfo.Function == "swapExactTokensForTokens" || swapInfo.Function == "swapTokensForExactTokens" {
			// Need to check token approval
			outputToken := swapInfo.TokenOut

			approvalTx, err = bot.checkAndApproveToken(ctx, outputToken, PancakeRouterV2, backrunInput)
			if err != nil {
				return fmt.Errorf("token approval check failed: %v, token: %s", err, outputToken.Hex())
			}

			// If approval needed, send and wait for it
			if approvalTx != nil {
				bot.logger.Info("Sending token approval transaction...")
				err = bot.client.SendTransaction(ctx, approvalTx)
				if err != nil {
					return fmt.Errorf("failed to send approval transaction: %v", err)
				}

				// Wait for approval tx to be mined
				receipt, err := bot.waitForTransaction(ctx, approvalTx.Hash(), bot.config.WaitForApprovalConfirmations)
				if err != nil {
					return fmt.Errorf("error waiting for approval transaction: %v", err)
				}

				if receipt.Status == 0 {
					return fmt.Errorf("approval transaction failed")
				}

				bot.logger.Info("Token approval transaction confirmed")
			}
		}
	}

	// Create backrun transaction
	backrunTx, err := bot.createBackrunTransaction(swapInfo, backrunInput, reversedPath)
	if err != nil {
		return fmt.Errorf("failed to create backrun transaction: %v", err)
	}
	bot.logger.Info("the backrun transaction hash: %s", backrunTx.Hash().Hex())

	// Simulate transaction if enabled
	if bot.config.SimulateTransactions {
		err = bot.simulateTransaction(ctx, bot.address, backrunTx)
		if err != nil {
			return fmt.Errorf("transaction simulation failed: %v", err)
		}
		bot.logger.Info("Backrun transaction simulation successful")
	}

	// Get raw transactions for the bundle
	origTxRaw, err := bot.getRawTransaction(swapInfo.TargetTx)
	if err != nil {
		return fmt.Errorf("failed to get raw original transaction: %v", err)
	}

	backrunTxRaw, err := bot.getRawTransaction(backrunTx)
	if err != nil {
		return fmt.Errorf("failed to get raw backrun transaction: %v", err)
	}

	// Create bundle
	bundle := BackrunBundle{
		Txs:               []string{origTxRaw, backrunTxRaw},
		RevertingTxHashes: []string{},
		MaxBlockNumber:    uint64(bot.blockNum.Load() + 10), // Valid for 2 blocks
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

	bot.logger.Info("submitting bundle with tx hash: %s at time: %s",
		swapInfo.TargetTx.Hash().Hex(), time.Now().Format("2006-01-02 15:04:05.000"))

	// Submit bundle
	bundleHash, err := bot.submitBundle(bundle, false, backrunTx.Nonce()+1, false)
	if err != nil {
		return fmt.Errorf("failed to submit bundle: %v", err)
	}

	bot.logger.Info("Successfully submitted bundle with hash: %s", bundleHash)
	return nil
}

// checkAndApproveToken checks token allowance and creates approval tx if needed
func (bot *MEVBot) checkAndApproveToken(ctx context.Context, tokenAddress, spenderAddress common.Address, amount *big.Int) (*types.Transaction, error) {
	// Parse ERC20 ABI
	tokenABI, err := abi.JSON(strings.NewReader(ERC20ABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ERC20 ABI: %v", err)
	}

	// Check current token balance
	balanceData, err := bot.callContractFunction(ctx, tokenAddress, tokenABI.Methods["balanceOf"], []interface{}{bot.address})
	if err != nil {
		return nil, fmt.Errorf("failed to check token balance: %v", err)
	}

	var balance *big.Int
	vals, err := tokenABI.Methods["balanceOf"].Outputs.Unpack(balanceData)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack balance: %v", err)
	}

	// Ensure vals contains the expected data and assign it to balance
	if len(vals) > 0 {
		balance, _ = vals[0].(*big.Int)
	} else {
		return nil, fmt.Errorf("unexpected empty result from balanceOf")
	}

	if balance.Cmp(amount) < 0 {
		return nil, fmt.Errorf("insufficient token balance: have %s, need %s",
			formatTokenAmount(balance), formatTokenAmount(amount))
	}

	// Check current allowance
	allowanceData, err := bot.callContractFunction(ctx, tokenAddress, tokenABI.Methods["allowance"],
		[]interface{}{bot.address, spenderAddress})
	if err != nil {
		return nil, fmt.Errorf("failed to check token allowance: %v", err)
	}

	var allowance *big.Int
	vals, err = tokenABI.Methods["allowance"].Outputs.Unpack(allowanceData)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack allowance: %v", err)
	}

	// Ensure vals contains the expected data and assign it to allowance
	if len(vals) > 0 {
		allowance, _ = vals[0].(*big.Int)
	} else {
		return nil, fmt.Errorf("unexpected empty result from allowance")
	}

	// If allowance is sufficient, no need for approval
	if allowance.Cmp(amount) >= 0 {
		bot.logger.Info("Token allowance sufficient: %s (needed: %s)",
			formatTokenAmount(allowance), formatTokenAmount(amount))
		return nil, nil
	}

	bot.logger.Info("Creating token approval transaction (current allowance: %s, needed: %s)",
		formatTokenAmount(allowance), formatTokenAmount(amount))

	// Create approval data
	// Use max uint256 value for unlimited approval
	maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

	approveData, err := tokenABI.Pack("approve", spenderAddress, maxUint256)
	if err != nil {
		return nil, fmt.Errorf("failed to pack approve call: %v", err)
	}

	// Create and sign transaction
	tx := types.NewTransaction(uint64(bot.nonce.Load()), tokenAddress, big.NewInt(0), 100000, big.NewInt(bot.gasPrice.Load()), approveData)

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(big.NewInt(bot.config.ChainID)), bot.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign approval transaction: %v", err)
	}

	return signedTx, nil
}

// createBackrunTransaction creates a backrun transaction for a given swap
func (bot *MEVBot) createBackrunTransaction(swapInfo *SwapInfo, backrunInput *big.Int, reversedPath []common.Address) (*types.Transaction, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Load PancakeSwap router ABI
	routerABI, err := abi.JSON(strings.NewReader(PancakeRouterABI))
	if err != nil {
		return nil, fmt.Errorf("failed to load router ABI: %v", err)
	}

	// Determine backrun strategy based on original swap
	var callData []byte
	var value = big.NewInt(0) // Default to 0 ETH value

	// Set deadline 5 minutes in the future
	deadline := big.NewInt(time.Now().Unix() + 300)

	switch swapInfo.Function {
	case "swapExactETHForTokens", "swapETHForExactTokens":
		// Original: ETH -> Token, Backrun: Token -> ETH
		// We need to set a minimum output amount, use 90% of estimated output
		estimatedOutput, err := bot.getAmountsOut(ctx, PancakeRouterV2, reversedPath, backrunInput)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate output: %v", err)
		}

		minOutput := new(big.Int).Mul(estimatedOutput[len(estimatedOutput)-1], big.NewInt(90))
		minOutput = new(big.Int).Div(minOutput, big.NewInt(100))

		// Pack swapExactTokensForETH call
		callData, err = routerABI.Pack("swapExactTokensForETH",
			backrunInput, minOutput, reversedPath, bot.address, deadline)
		if err != nil {
			return nil, fmt.Errorf("failed to pack swapExactTokensForETH call: %v", err)
		}

	case "swapExactTokensForETH", "swapTokensForExactETH":
		// Original: Token -> ETH, Backrun: ETH -> Token
		// We need to set a minimum output amount, use 90% of estimated output
		estimatedOutput, err := bot.getAmountsOut(ctx, PancakeRouterV2, reversedPath, backrunInput)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate output: %v", err)
		}

		minOutput := new(big.Int).Mul(estimatedOutput[len(estimatedOutput)-1], big.NewInt(90))
		minOutput = new(big.Int).Div(minOutput, big.NewInt(100))

		// Pack swapExactETHForTokens call
		callData, err = routerABI.Pack("swapExactETHForTokens",
			minOutput, reversedPath, bot.address, deadline)
		if err != nil {
			return nil, fmt.Errorf("failed to pack swapExactETHForTokens call: %v", err)
		}

		// Set transaction value (ETH amount to swap)
		value = backrunInput

	case "swapExactTokensForTokens", "swapTokensForExactTokens":
		// Original: Token A -> Token B, Backrun: Token B -> Token A
		// We need to set a minimum output amount, use 90% of estimated output
		estimatedOutput, err := bot.getAmountsOut(ctx, PancakeRouterV2, reversedPath, backrunInput)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate output: %v", err)
		}

		minOutput := new(big.Int).Mul(estimatedOutput[len(estimatedOutput)-1], big.NewInt(90))
		minOutput = new(big.Int).Div(minOutput, big.NewInt(100))

		// Pack swapExactTokensForTokens call
		callData, err = routerABI.Pack("swapExactTokensForTokens",
			backrunInput, minOutput, reversedPath, bot.address, deadline)
		if err != nil {
			return nil, fmt.Errorf("failed to pack swapExactTokensForTokens call: %v", err)
		}
	}

	// Create dynamic fee transaction instead of legacy
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   big.NewInt(bot.config.ChainID),
		Nonce:     uint64(bot.nonce.Load()),
		To:        &PancakeRouterV2,
		Value:     value,
		Gas:       bot.config.GasLimit,
		GasTipCap: big.NewInt(bot.gasTip.Load()),
		GasFeeCap: big.NewInt(bot.gasPrice.Load()),
		Data:      callData,
	})

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewLondonSigner(big.NewInt(bot.config.ChainID)), bot.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign transaction: %v", err)
	}

	return signedTx, nil
}

func (bot *MEVBot) createBlockRazorBackrunTransaction(swapInfo *SwapInfo, backrunInput *big.Int, reversedPath []common.Address, proxyBidContract common.Address, refundAddress common.Address, refundCfg uint64, bidValue *big.Int) (*types.Transaction, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Load our backrun contract ABI
	backrunContractABI, err := abi.JSON(strings.NewReader(BackrunABI))
	if err != nil {
		return nil, fmt.Errorf("failed to load backrun contract ABI: %v", err)
	}

	// Determine backrun strategy parameters based on original swap
	var backrunMethod string
	var backrunParams []interface{}
	var txValue = bidValue // Default to bid value only

	// Set deadline 5 minutes in the future
	deadline := big.NewInt(time.Now().Unix() + 300)

	switch swapInfo.Function {
	case "swapExactETHForTokens", "swapETHForExactTokens":
		// Original: ETH -> Token, Backrun: Token -> ETH
		// We need to set a minimum output amount, use 90% of estimated output
		estimatedOutput, err := bot.getAmountsOut(ctx, PancakeRouterV2, reversedPath, backrunInput)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate output: %v", err)
		}

		minOutput := new(big.Int).Mul(estimatedOutput[len(estimatedOutput)-1], big.NewInt(90))
		minOutput = new(big.Int).Div(minOutput, big.NewInt(100))

		backrunMethod = "executeBackrunTokenToETH"
		backrunParams = []interface{}{
			PancakeRouterV2,
			backrunInput,
			minOutput,
			reversedPath,
			deadline,
			proxyBidContract,
			refundAddress,
			big.NewInt(int64(refundCfg)),
			bidValue,
		}
	case "swapExactTokensForETH", "swapTokensForExactETH":
		// Original: Token -> ETH, Backrun: ETH -> Token
		// We need to set a minimum output amount, use 90% of estimated output
		estimatedOutput, err := bot.getAmountsOut(ctx, PancakeRouterV2, reversedPath, backrunInput)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate output: %v", err)
		}

		minOutput := new(big.Int).Mul(estimatedOutput[len(estimatedOutput)-1], big.NewInt(90))
		minOutput = new(big.Int).Div(minOutput, big.NewInt(100))

		// Add the ETH amount for the swap to the transaction value
		txValue = new(big.Int).Add(bidValue, backrunInput)

		backrunMethod = "executeBackrunETHToToken"
		backrunParams = []interface{}{
			PancakeRouterV2,
			minOutput,
			reversedPath,
			deadline,
			proxyBidContract,
			refundAddress,
			big.NewInt(int64(refundCfg)),
			bidValue,
		}
	case "swapExactTokensForTokens", "swapTokensForExactTokens":
		// Original: Token A -> Token B, Backrun: Token B -> Token A
		// We need to set a minimum output amount, use 90% of estimated output
		estimatedOutput, err := bot.getAmountsOut(ctx, PancakeRouterV2, reversedPath, backrunInput)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate output: %v", err)
		}

		minOutput := new(big.Int).Mul(estimatedOutput[len(estimatedOutput)-1], big.NewInt(90))
		minOutput = new(big.Int).Div(minOutput, big.NewInt(100))

		backrunMethod = "executeBackrunTokenToToken"
		backrunParams = []interface{}{
			PancakeRouterV2,
			backrunInput,
			minOutput,
			reversedPath,
			deadline,
			proxyBidContract,
			refundAddress,
			big.NewInt(int64(refundCfg)),
			bidValue,
		}
	}
	bot.logger.Info("backrunMethod: %s, backrunParams: %v", backrunMethod, backrunParams)

	// Pack the call data for our backrun contract
	callData, err := backrunContractABI.Pack(backrunMethod, backrunParams...)
	if err != nil {
		return nil, fmt.Errorf("failed to pack %s call: %v", backrunMethod, err)
	}

	// Create dynamic fee transaction instead of legacy
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   big.NewInt(bot.config.ChainID),
		Nonce:     uint64(bot.nonce.Load()),
		To:        &bot.config.BackrunContractAddress, // Address of our backrun contract
		Value:     txValue,
		Gas:       bot.config.GasLimit,
		GasTipCap: big.NewInt(bot.gasTip.Load()),
		GasFeeCap: big.NewInt(bot.gasPrice.Load()),
		Data:      callData,
	})

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewLondonSigner(big.NewInt(bot.config.ChainID)), bot.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign transaction: %v", err)
	}

	return signedTx, nil
}

// getAmountsOut calls the router's getAmountsOut function
func (bot *MEVBot) getAmountsOut(ctx context.Context, routerAddress common.Address, path []common.Address, amountIn *big.Int) ([]*big.Int, error) {
	// Validate parameters
	if amountIn == nil || amountIn.Cmp(big.NewInt(0)) <= 0 {
		log.Printf("Invalid input amount: %v", amountIn)
		return nil, fmt.Errorf("invalid input amount: must be positive")
	}

	if len(path) < 2 {
		log.Printf("Invalid path length: %d", len(path))
		return nil, fmt.Errorf("invalid path: must contain at least 2 tokens")
	}

	// Check token addresses
	for i, addr := range path {
		if addr == (common.Address{}) {
			log.Printf("Invalid token address at index %d: %v", i, addr)
			return nil, fmt.Errorf("invalid token address at index %d", i)
		}
	}

	// Load router ABI
	routerABI, err := abi.JSON(strings.NewReader(PancakeRouterABI))
	if err != nil {
		log.Printf("Failed to load router ABI: %v", err)
		return nil, fmt.Errorf("failed to load router ABI: %v", err)
	}

	// Pack call data
	data, err := routerABI.Pack("getAmountsOut", amountIn, path)
	if err != nil {
		log.Printf("Failed to pack getAmountsOut call: %v", err)
		return nil, fmt.Errorf("failed to pack getAmountsOut call: %v", err)
	}

	// Create call message
	msg := ethereum.CallMsg{
		To:   &routerAddress,
		Data: data,
	}

	// Execute call
	result, err := bot.client.CallContract(ctx, msg, nil)
	if err != nil {
		log.Printf("Contract call failed: %v", err)
		return nil, fmt.Errorf("contract call failed: %v", err)
	}

	// Unpack result
	var amountsOut []*big.Int
	err = routerABI.UnpackIntoInterface(&amountsOut, "getAmountsOut", result)
	if err != nil {
		log.Printf("Failed to unpack result: %v", err)
		return nil, fmt.Errorf("failed to unpack result: %v", err)
	}

	// Validate output
	if len(amountsOut) != len(path) {
		log.Printf("Unexpected amounts length: got %d, want %d", len(amountsOut), len(path))
		return nil, fmt.Errorf("unexpected amounts length: got %d, want %d", len(amountsOut), len(path))
	}

	return amountsOut, nil
}

// simulateTransaction simulates a transaction execution
func (bot *MEVBot) simulateTransaction(ctx context.Context, from common.Address, tx *types.Transaction) error {
	msg := ethereum.CallMsg{
		From:     from,
		To:       tx.To(),
		Gas:      tx.Gas(),
		GasPrice: tx.GasPrice(),
		Value:    tx.Value(),
		Data:     tx.Data(),
	}

	_, err := bot.client.CallContract(ctx, msg, nil)
	return err
}

// getRawTransaction converts a transaction to hex format
func (bot *MEVBot) getRawTransaction(tx *types.Transaction) (string, error) {
	data, err := tx.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("failed to marshal transaction: %v", err)
	}
	return hexutil.Encode(data), nil
}

// callBundle simulate a bundle
func (bot *MEVBot) callBundle(bundle BackrunBundle, nonce uint64, backrunBundle bool) error {
	builderURLs := strings.Split(bot.config.BuildersRpcURLs, ",")

	// Use wait group to wait for all goroutines to complete
	var wg sync.WaitGroup
	var mu sync.Mutex // Mutex to protect shared slices
	errs := make([]error, 0)

	// Send bundle to each builder
	for i, url := range builderURLs {
		if url = strings.TrimSpace(url); url == "" {
			continue
		}

		wg.Add(1)
		go func(index int, builderURL string) {
			defer wg.Done()
			copiedBundle := bundle
			var request JsonRpcRequest
			if !backrunBundle {
				// add a transfer tx to the copied bundle
				transferTx, err := bot.createTransferTx(nonce, bot.config.BuildersEoaAddress[i], big.NewInt(200000000000000)) //0.00002  $0.1
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("failed to create transfer transaction: %v", err))
					mu.Unlock()
					return
				}
				transferTxBz, err := bot.getRawTransaction(transferTx)
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("failed to get raw transaction: %v", err))
					mu.Unlock()
					return
				}
				copiedBundle.Txs = append(copiedBundle.Txs, transferTxBz)

			}
			request = JsonRpcRequest{
				JsonRpc: "2.0",
				Method:  "eth_callBundle",
				Params:  []interface{}{copiedBundle},
				ID:      1,
			}

			jsonData, err := json.Marshal(request)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to marshal JSON-RPC request: %v", err))
				mu.Unlock()
				return
			}

			// Send request to this builder URL
			resp, err := http.Post(
				builderURL,
				"application/json",
				bytes.NewBuffer(jsonData),
			)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to call bundle to %s: %v", builderURL, err))
				mu.Unlock()
				return
			}
			defer resp.Body.Close()

			// Read response
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to read response from %s: %v, body: %s", builderURL, err, string(body)))
				mu.Unlock()
				return
			}

			// Parse response
			var response JsonRpcResponse
			err = json.Unmarshal(body, &response)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to parse response from %s: %v, body: %s", builderURL, err, string(body)))
				mu.Unlock()
				return
			}

			// Check for errors
			if response.Error != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("relay error from %s: %v", builderURL, response.Error))
				mu.Unlock()
				return
			}

			bot.logger.Info("Bundle called successfully to %s, response: %s", builderURL, string(body))
		}(i, url)
	}

	// Wait for all requests to complete
	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("all bundle calls failed: %v", errs)
	}

	return nil
}

// submitBundle submits a bundle to multiple relays
func (bot *MEVBot) submitBundle(bundle BackrunBundle, skipSend bool, nonce uint64, backrunBundle bool) (string, error) {
	if skipSend {
		bot.logger.Debug("Skipping sending bundle to relay")
		return "", nil
	}

	// Convert comma-separated BuildersRpcURLs to slice
	builderURLs := strings.Split(bot.config.BuildersRpcURLs, ",")

	// Use wait group to wait for all goroutines to complete
	var wg sync.WaitGroup
	var mu sync.Mutex // Mutex to protect shared slices
	responses := make([]string, 0)
	errs := make([]error, 0)

	// Send bundle to each builder
	for i, url := range builderURLs {
		if url = strings.TrimSpace(url); url == "" {
			continue
		}

		wg.Add(1)
		go func(index int, builderURL string) {
			defer wg.Done()
			copiedBundle := bundle
			var request JsonRpcRequest
			if !backrunBundle {
				// add a transfer tx to the copied bundle
				transferTx, err := bot.createTransferTx(nonce, bot.config.BuildersEoaAddress[i], big.NewInt(200000000000000)) //0.00002  $0.1
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("failed to create transfer transaction: %v", err))
					mu.Unlock()
					return
				}
				transferTxBz, err := bot.getRawTransaction(transferTx)
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("failed to get raw transaction: %v", err))
					mu.Unlock()
					return
				}
				copiedBundle.Txs = append(copiedBundle.Txs, transferTxBz)
				request = JsonRpcRequest{
					JsonRpc: "2.0",
					Method:  "eth_sendBundle",
					Params:  []interface{}{copiedBundle},
					ID:      1,
				}
			} else {
				request = JsonRpcRequest{
					JsonRpc: "2.0",
					Method:  "eth_sendMevBundle",
					Params:  []interface{}{copiedBundle},
					ID:      1,
				}
			}

			jsonData, err := json.Marshal(request)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to marshal JSON-RPC request: %v", err))
				mu.Unlock()
				return
			}

			// Send request to this builder URL
			resp, err := http.Post(
				builderURL,
				"application/json",
				bytes.NewBuffer(jsonData),
			)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to send bundle to %s: %v", builderURL, err))
				mu.Unlock()
				return
			}
			defer resp.Body.Close()

			// Read response
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to read response from %s: %v, body: %s", builderURL, err, string(body)))
				mu.Unlock()
				return
			}

			// Parse response
			var response JsonRpcResponse
			err = json.Unmarshal(body, &response)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to parse response from %s: %v, body: %s", builderURL, err, string(body)))
				mu.Unlock()
				return
			}

			// Check for errors
			if response.Error != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("relay error from %s: %v", builderURL, response.Error))
				mu.Unlock()
				return
			}

			// Get bundle hash
			bundleHash, ok := response.Result.(string)
			if !ok {
				mu.Lock()
				errs = append(errs, fmt.Errorf("invalid response format from %s", builderURL))
				mu.Unlock()
				return
			}

			mu.Lock()
			responses = append(responses, bundleHash)
			mu.Unlock()
			bot.logger.Info("Bundle submitted successfully to %s, response: %s", builderURL, string(body))
		}(i, url)
	}

	// Wait for all requests to complete
	wg.Wait()

	// If all requests failed, return error
	if len(responses) == 0 {
		return "", fmt.Errorf("all bundle submissions failed: %v", errs)
	}

	// Return first successful response
	return responses[0], nil
}

// createTransferTx creates a transfer transaction to the builder EOA
func (bot *MEVBot) createTransferTx(nonce uint64, to common.Address, amount *big.Int) (*types.Transaction, error) {
	tx := types.NewTransaction(
		nonce,
		to,
		amount,
		bot.config.GasLimit,
		big.NewInt(bot.gasPrice.Load()),
		nil,
	)
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(big.NewInt(bot.config.ChainID)), bot.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign transfer transaction: %v", err)
	}

	// Simulate the transaction if simulation is enabled
	if bot.config.SimulateTransactions {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err = bot.simulateTransaction(ctx, bot.address, signedTx); err != nil {
			return nil, fmt.Errorf("transfer transaction simulation failed: %v", err)
		}
		bot.logger.Info("Transfer transaction simulation successful")
	}

	bot.logger.Info("Created transfer transaction to builder EOA: %s, amount: %s BNB",
		to.Hex(),
		formatEthValue(amount))

	return signedTx, nil
}

// waitForTransaction waits for a transaction to be confirmed
func (bot *MEVBot) waitForTransaction(ctx context.Context, txHash common.Hash, confirmations uint64) (*types.Receipt, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			receipt, err := bot.client.TransactionReceipt(ctx, txHash)
			if err != nil {
				if errors.Is(err, ethereum.NotFound) {
					continue
				}
				return nil, err
			}

			// Get current block number
			currentBlock, err := bot.client.BlockNumber(ctx)
			if err != nil {
				return nil, err
			}

			// Check confirmations
			if currentBlock >= receipt.BlockNumber.Uint64()+confirmations {
				return receipt, nil
			}

			// Not enough confirmations yet, continue waiting
			bot.logger.Debug("Waiting for transaction %s to reach %d confirmations (current: %d)",
				txHash.Hex(), confirmations, currentBlock-receipt.BlockNumber.Uint64())
		}
	}
}

// callContractFunction is a helper to call contract functions
func (bot *MEVBot) callContractFunction(ctx context.Context, contractAddress common.Address, method abi.Method, args []interface{}) ([]byte, error) {
	// Pack function parameters
	input, err := method.Inputs.Pack(args...)
	if err != nil {
		return nil, fmt.Errorf("failed to pack input parameters: %v", err)
	}

	// Create call data with function selector
	data := append(method.ID, input...)

	// Create call message
	msg := ethereum.CallMsg{
		To:   &contractAddress,
		Data: data,
	}

	// Execute call
	return bot.client.CallContract(ctx, msg, nil)
}

// isTokenSafe checks if a token is safe to trade
func (bot *MEVBot) isTokenSafe(tokenAddress common.Address) bool {
	// A real implementation would check for suspicious functions in the contract code,
	// high buy/sell taxes, historical sell transactions, and contract audits
	return true
}

// Utility Functions

// isPancakeSwapFunction checks if a function selector corresponds to a PancakeSwap swap function
func isPancakeSwapFunction(functionSelector string) bool {
	// List of PancakeSwap swap function selectors
	swapSelectors := map[string]bool{
		SwapExactETHForTokens:    true,
		SwapExactTokensForETH:    true,
		SwapExactTokensForTokens: true,
		SwapETHForExactTokens:    true,
		SwapTokensForExactETH:    true,
		SwapTokensForExactTokens: true,
	}

	return swapSelectors[functionSelector]
}

// formatTokenAmount formats a token amount with 6 decimal places
func formatTokenAmount(amount *big.Int) string {
	if amount == nil {
		return "0"
	}

	f := new(big.Float).SetInt(amount)
	f = new(big.Float).Quo(f, big.NewFloat(1e18)) // Assuming 18 decimals

	return f.Text('f', 6)
}

// formatEthValue formats a wei amount as ETH with 6 decimal places
func formatEthValue(wei *big.Int) string {
	if wei == nil {
		return "0"
	}

	f := new(big.Float).SetInt(wei)
	f = new(big.Float).Quo(f, big.NewFloat(1e18))

	return f.Text('f', 6)
}

// loadConfig loads configuration from environment variables
func loadConfig() Config {
	// Load environment variables
	_ = godotenv.Load()

	// Parse router addresses from environment
	routerAddressesStr := os.Getenv("ROUTER_ADDRESSES_V2")
	if routerAddressesStr == "" {
		// Default to PancakeSwap V2 Router
		routerAddressesStr = "0x10ED43C718714eb63d5aA57B78B54704E256024E"
	}
	routerAddressStrs := strings.Split(routerAddressesStr, ",")
	routerAddresses := make(map[string]bool)
	for _, addrStr := range routerAddressStrs {
		routerAddresses[strings.ToLower(strings.TrimSpace(addrStr))] = true
	}

	// Parse minimum profit threshold
	minProfitThresholdStr := os.Getenv("MIN_PROFIT_THRESHOLD")
	minProfitThreshold := big.NewInt(0)
	if minProfitThresholdStr != "" {
		minProfitThreshold.SetString(minProfitThresholdStr, 10)
	} else {
		// Default to 0.01 BNB
		minProfitThreshold = big.NewInt(10000000000000000)
	}

	// Parse ChainID
	chainIDStr := os.Getenv("CHAIN_ID")
	chainID := int64(56) // Default to BSC mainnet
	if chainIDStr != "" {
		var success bool
		chainIDBig, success := new(big.Int).SetString(chainIDStr, 10)
		if !success {
			log.Fatalf("Invalid CHAIN_ID: %s", chainIDStr)
		}
		chainID = chainIDBig.Int64()
	}

	// Use the correct RPC URL for BlockRazor
	buildersRpcURLs := os.Getenv("BUILDERS_RPC_URLS")

	// Parse polling interval
	pollIntervalStr := os.Getenv("POLL_INTERVAL")
	pollInterval := 500 * time.Millisecond // Default to 500ms
	if pollIntervalStr != "" {
		if interval, err := time.ParseDuration(pollIntervalStr); err == nil {
			pollInterval = interval
		}
	}

	buildersEoa := os.Getenv("BUILDERS_EOA")
	if buildersEoa == "" {
		log.Fatal("BUILDERS_EOA environment variable is required")
	}
	builders := make([]common.Address, 0)
	for _, addr := range strings.Split(buildersEoa, ",") {
		builders = append(builders, common.HexToAddress(addr))
	}

	// Parse log level
	logLevelStr := os.Getenv("LOG_LEVEL")
	logLevel := INFO // Default to INFO
	switch strings.ToUpper(logLevelStr) {
	case "DEBUG":
		logLevel = DEBUG
	case "INFO":
		logLevel = INFO
	case "WARNING":
		logLevel = WARNING
	case "ERROR":
		logLevel = ERROR
	}

	// Parse gas price settings
	maxGasPriceStr := os.Getenv("MAX_GAS_PRICE")
	maxGasPrice := big.NewInt(20000000000) // Default to 20 Gwei
	if maxGasPriceStr != "" {
		var success bool
		maxGasPrice, success = new(big.Int).SetString(maxGasPriceStr, 10)
		if !success {
			log.Printf("Invalid MAX_GAS_PRICE: %s, using default", maxGasPriceStr)
		}
	}

	gasPriceMultiplierStr := os.Getenv("GAS_PRICE_MULTIPLIER")
	gasPriceMultiplier := int64(120) // Default to 1.2x
	if gasPriceMultiplierStr != "" {
		gasPriceMultiplier64, ok := new(big.Float).SetString(gasPriceMultiplierStr)
		if ok {
			gasPriceMultiplier, _ = gasPriceMultiplier64.Mul(gasPriceMultiplier64, big.NewFloat(100)).Int64()
		} else {
			log.Printf("Invalid GAS_PRICE_MULTIPLIER: %s, using default", gasPriceMultiplierStr)
		}
	}

	// Parse gas limit
	gasLimitStr := os.Getenv("GAS_LIMIT")
	gasLimit := uint64(500000) // Default
	if gasLimitStr != "" {
		if parsedGasLimit, err := new(big.Int).SetString(gasLimitStr, 10); err {
			gasLimit = parsedGasLimit.Uint64()
		}
	}

	// Parse simulation setting
	simulateStr := os.Getenv("SIMULATE_TRANSACTIONS")
	simulateTransactions := true // Default to true
	if strings.ToLower(simulateStr) == "false" {
		simulateTransactions = false
	}

	// Parse approval confirmations
	waitForApprovalStr := os.Getenv("WAIT_FOR_APPROVAL_CONFIRMATIONS")
	waitForApproval := uint64(1) // Default to 1 confirmation
	if waitForApprovalStr != "" {
		if parsed, err := new(big.Int).SetString(waitForApprovalStr, 10); err {
			waitForApproval = parsed.Uint64()
		}
	}

	monitorPairsStr := os.Getenv("MONITOR_PAIRS")
	var paris []TokenPair

	if monitorPairsStr != "" {
		monitorPairStrs := strings.Split(monitorPairsStr, ",")
		for _, pairStr := range monitorPairStrs {
			pairStr = strings.TrimSpace(pairStr)
			if pairStr == "" {
				continue
			}

			tokenPair := strings.Split(pairStr, ":")
			if len(tokenPair) != 2 {
				log.Printf("Invalid pair format: %s, expected TOKEN1:TOKEN2", pairStr)
				continue
			}

			token1Str := strings.TrimSpace(tokenPair[0])
			token2Str := strings.TrimSpace(tokenPair[1])

			if !common.IsHexAddress(token1Str) || !common.IsHexAddress(token2Str) {
				log.Printf("Invalid address in pair: %s", pairStr)
				continue
			}

			token1 := common.HexToAddress(token1Str)
			token2 := common.HexToAddress(token2Str)

			paris = append(paris, TokenPair{
				Token1: token1,
				Token2: token2,
			})
		}
	}

	skipCheckApprovalStr := os.Getenv("SKIP_CHECK_APPROVAL")
	skipCheckApproval := false // Default to false
	if strings.ToLower(skipCheckApprovalStr) == "true" {
		skipCheckApproval = true
	}

	// Parse gas tip settings
	maxGasTipStr := os.Getenv("MAX_GAS_TIP")
	maxGasTip := big.NewInt(2000000000) // Default to 2 Gwei
	if maxGasTipStr != "" {
		var success bool
		maxGasTip, success = new(big.Int).SetString(maxGasTipStr, 10)
		if !success {
			log.Printf("Invalid MAX_GAS_TIP: %s, using default", maxGasTipStr)
		}
	}

	gasTipMultiplierStr := os.Getenv("GAS_TIP_MULTIPLIER")
	gasTipMultiplier := int64(120) // Default to 1.2x
	if gasTipMultiplierStr != "" {
		gasTipMultiplier64, ok := new(big.Float).SetString(gasTipMultiplierStr)
		if ok {
			gasTipMultiplier, _ = gasTipMultiplier64.Mul(gasTipMultiplier64, big.NewFloat(100)).Int64()
		} else {
			log.Printf("Invalid GAS_TIP_MULTIPLIER: %s, using default", gasTipMultiplierStr)
		}
	}

	backrunContractAddress := os.Getenv("BACKRUN_CONTRACT_ADDRESS")
	if backrunContractAddress == "" {
		log.Fatal("BACKRUN_CONTRACT_ADDRESS environment variable is required")
	}
	backrunContract := common.HexToAddress(backrunContractAddress)

	return Config{
		BuildersRpcURLs:              buildersRpcURLs,
		NodeURL:                      os.Getenv("NODE_URL"),
		WSURL:                        os.Getenv("WS_URL"),
		ScutumURL:                    os.Getenv("SCUTUM_URL"),
		PrivateKey:                   os.Getenv("PRIVATE_KEY"),
		ChainID:                      chainID,
		MinProfitThreshold:           minProfitThreshold,
		RouterAddresses:              routerAddresses,
		RefundAddress:                common.HexToAddress(os.Getenv("REFUND_ADDRESS")),
		PollInterval:                 pollInterval,
		LogLevel:                     logLevel,
		MaxGasPrice:                  maxGasPrice,
		GasPriceMultiplier:           gasPriceMultiplier,
		GasLimit:                     gasLimit,
		SimulateTransactions:         simulateTransactions,
		WaitForApprovalConfirmations: waitForApproval,
		MonitorPairs:                 paris,
		SkipApprovalCheck:            skipCheckApproval,
		MaxGasTip:                    maxGasTip,
		GasTipMultiplier:             gasTipMultiplier,
		BuildersEoaAddress:           builders,
		BackrunContractAddress:       backrunContract,
	}
}

func main() {
	// Load configuration
	config := loadConfig()

	// Create MEV bot instance
	bot, err := NewMEVBot(config)
	if err != nil {
		log.Fatalf("Failed to initialize MEV bot: %v", err)
	}

	// Start the bot
	err = bot.Start()
	if err != nil {
		log.Fatalf("MEV bot error: %v", err)
	}
}

// findOptimalBackrunAmount calculates the most profitable input amount for a backrun
func (bot *MEVBot) findOptimalBackrunAmount(swapInfo *SwapInfo) (*big.Int, *big.Int, error) {
	// Create reversed path for the backrun
	bot.logger.Debug("Reversing path for backrun calculation")
	reversedPath := make([]common.Address, len(swapInfo.Path))
	for i, addr := range swapInfo.Path {
		reversedPath[len(swapInfo.Path)-1-i] = addr
	}
	bot.logger.Debug("Reversed path: %v", reversedPath)

	// Determine the backrun strategy based on the original swap function
	var strategy string
	var maxBackrunInput *big.Int

	switch swapInfo.Function {
	case "swapExactETHForTokens", "swapETHForExactTokens":
		strategy = "TokenToETH"
		bot.logger.Info("Backrun strategy determined: %s", strategy)

		amountsOut, err := bot.getAmountsOut(context.Background(), PancakeRouterV2, swapInfo.Path, swapInfo.AmountIn)
		if err != nil {
			bot.logger.Error("Failed to get expected output amounts: %v", err)
			return nil, nil, fmt.Errorf("failed to get expected output amounts: %v", err)
		}
		maxBackrunInput = amountsOut[len(amountsOut)-1]
		bot.logger.Debug("Max backrun input (TokenToETH): %s", maxBackrunInput.String())

	case "swapExactTokensForETH", "swapTokensForExactETH":
		strategy = "ETHToToken"
		bot.logger.Info("Backrun strategy determined: %s", strategy)

		amountsOut, err := bot.getAmountsOut(context.Background(), PancakeRouterV2, swapInfo.Path, swapInfo.AmountIn)
		if err != nil {
			bot.logger.Error("Failed to get expected output amounts: %v", err)
			return nil, nil, fmt.Errorf("failed to get expected output amounts: %v", err)
		}
		maxBackrunInput = amountsOut[len(amountsOut)-1]
		bot.logger.Debug("Max backrun input (ETHToToken): %s", maxBackrunInput.String())

	case "swapExactTokensForTokens", "swapTokensForExactTokens":
		strategy = "TokenToToken"
		bot.logger.Info("Backrun strategy determined: %s", strategy)

		amountsOut, err := bot.getAmountsOut(context.Background(), PancakeRouterV2, swapInfo.Path, swapInfo.AmountIn)
		if err != nil {
			bot.logger.Error("Failed to get expected output amounts: %v", err)
			return nil, nil, fmt.Errorf("failed to get expected output amounts: %v", err)
		}
		maxBackrunInput = amountsOut[len(amountsOut)-1]
		bot.logger.Debug("Max backrun input (TokenToToken): %s", maxBackrunInput.String())
	}

	// Perform binary search to find optimal input amount
	bot.logger.Info("Starting binary search for optimal input amount")
	//percentages := []int{10, 20, 30, 40, 50, 60, 70, 80, 90}
	percentages := []int{10}
	var bestInput, bestProfit *big.Int

	for _, percentage := range percentages {
		inputAmount := new(big.Int).Mul(maxBackrunInput, big.NewInt(int64(percentage)))
		inputAmount = new(big.Int).Div(inputAmount, big.NewInt(100))

		if inputAmount.Cmp(big.NewInt(1000)) <= 0 {
			bot.logger.Debug("Skipping input amount %s (too small)", inputAmount.String())
			continue
		}

		bot.logger.Debug("Calculating profit for input amount: %s (%d%% of max)", inputAmount.String(), percentage)
		profit, err := bot.calculateBackrunProfit(strategy, inputAmount, reversedPath, swapInfo)
		if err != nil {
			bot.logger.Warning("Error calculating profit for %d%% input: %v", percentage, err)
			continue
		}

		bot.logger.Debug("Profit for input amount %s: %s", inputAmount.String(), profit.String())
		if bestProfit == nil || profit.Cmp(bestProfit) > 0 {
			bot.logger.Info("New best profit found: %s with input amount: %s", profit.String(), inputAmount.String())
			bestProfit = profit
			bestInput = inputAmount
		}
	}

	if bestInput != nil && bestProfit.Cmp(big.NewInt(0)) > 0 {
		bot.logger.Debug("best input amount: %s", bestInput.String())
	} else {
		bot.logger.Warning("No profitable backrun opportunity found")
	}

	bot.logger.Info("Optimal backrun calculation completed. Best input: %s, Best profit: %s", bestInput, bestProfit)
	return bestInput, bestProfit, nil
}

func (bot *MEVBot) calculateBackrunProfit(strategy string, inputAmount *big.Int, path []common.Address, victimSwapInfo *SwapInfo) (*big.Int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. get reverses before the victim transaction happen
	initialReserves, err := bot.getPathReserves(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get initial reserves: %v", err)
	}

	// 2. without the victim transaction, what we will get
	normalOutput, err := bot.getAmountsOutWithReserves(initialReserves, path, inputAmount)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate normal output: %v", err)
	}

	// 3. simulate the victim transaction effect on the reserves
	updatedReserves, err := bot.simulateVictimTransaction(ctx, initialReserves, victimSwapInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to simulate victim transaction: %v", err)
	}

	// 4. calculate the backrun output with the updated reserves
	backrunOutput, err := bot.getAmountsOutWithReserves(updatedReserves, path, inputAmount)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate backrun output: %v", err)
	}

	// 5. calculate profit
	var profit *big.Int

	switch strategy {
	case "TokenToETH":
		normalETH := normalOutput[len(normalOutput)-1]
		backrunETH := backrunOutput[len(backrunOutput)-1]
		profit = new(big.Int).Sub(backrunETH, normalETH)

	case "ETHToToken":
		normalToken := normalOutput[len(normalOutput)-1]
		backrunToken := backrunOutput[len(backrunOutput)-1]
		extraToken := new(big.Int).Sub(backrunToken, normalToken)

		if extraToken.Sign() > 0 {
			extraValueInETH, err := bot.getTokenValueInETH(ctx, path[len(path)-1], extraToken)
			if err != nil {
				return nil, fmt.Errorf("failed to get extra token value: %v", err)
			}
			profit = extraValueInETH
		} else {
			profit = big.NewInt(0)
		}

	case "TokenToToken":
		normalToken := normalOutput[len(normalOutput)-1]
		backrunToken := backrunOutput[len(backrunOutput)-1]
		extraToken := new(big.Int).Sub(backrunToken, normalToken)

		if extraToken.Sign() > 0 {
			extraValueInETH, err := bot.getTokenValueInETH(ctx, path[len(path)-1], extraToken)
			if err != nil {
				return nil, fmt.Errorf("failed to get extra token value: %v", err)
			}
			profit = extraValueInETH
		} else {
			profit = big.NewInt(0)
		}
	}
	gasCost := bot.calculateGasCost()
	netProfit := new(big.Int).Sub(profit, gasCost)

	bot.logger.Info("Strategy: %s, Input: %s, Normal output: %s, Backrun output: %s",
		strategy, inputAmount, normalOutput[len(normalOutput)-1], backrunOutput[len(backrunOutput)-1])
	bot.logger.Info("Raw profit: %s, Gas cost: %s, Net profit: %s",
		profit, gasCost, netProfit)

	return netProfit, nil
}

type PairReserves struct {
	TokenA   common.Address
	TokenB   common.Address
	ReserveA *big.Int
	ReserveB *big.Int
}

func (bot *MEVBot) getPathReserves(ctx context.Context, path []common.Address) ([]*PairReserves, error) {
	reserves := make([]*PairReserves, len(path)-1)

	for i := 0; i < len(path)-1; i++ {
		tokenA := path[i]
		tokenB := path[i+1]

		pairAddr, err := bot.getPairAddress(ctx, PancakeFactoryV2, tokenA, tokenB)
		if err != nil {
			return nil, fmt.Errorf("failed to get pair address for %s-%s: %v",
				tokenA.Hex(), tokenB.Hex(), err)
		}
		reserve0, reserve1, err := bot.getReservesFromPair(ctx, pairAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to get reserves for pair %s: %v",
				pairAddr.Hex(), err)
		}

		token0, err := bot.getToken0(ctx, pairAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to get token0 for pair %s: %v",
				pairAddr.Hex(), err)
		}

		if token0 == tokenA {
			reserves[i] = &PairReserves{
				TokenA:   tokenA,
				TokenB:   tokenB,
				ReserveA: reserve0,
				ReserveB: reserve1,
			}
		} else {
			reserves[i] = &PairReserves{
				TokenA:   tokenA,
				TokenB:   tokenB,
				ReserveA: reserve1,
				ReserveB: reserve0,
			}
		}
	}

	return reserves, nil
}

// getAmountsOutWithReserves 使用指定的储备计算交易输出
func (bot *MEVBot) getAmountsOutWithReserves(reserves []*PairReserves, path []common.Address, amountIn *big.Int) ([]*big.Int, error) {
	if len(reserves) != len(path)-1 {
		return nil, fmt.Errorf("reserves length (%d) does not match path length-1 (%d)",
			len(reserves), len(path)-1)
	}

	amounts := make([]*big.Int, len(path))
	amounts[0] = amountIn

	for i := 0; i < len(path)-1; i++ {
		pair := reserves[i]

		// 验证储备匹配路径
		if (pair.TokenA != path[i] || pair.TokenB != path[i+1]) &&
			(pair.TokenB != path[i] || pair.TokenA != path[i+1]) {
			return nil, fmt.Errorf("reserve at index %d does not match path tokens %s-%s",
				i, path[i].Hex(), path[i+1].Hex())
		}

		// 确保正确的方向
		var reserveIn, reserveOut *big.Int
		if pair.TokenA == path[i] {
			reserveIn = pair.ReserveA
			reserveOut = pair.ReserveB
		} else {
			reserveIn = pair.ReserveB
			reserveOut = pair.ReserveA
		}

		// 计算输出时考虑 0.3% 手续费
		amountInWithFee := new(big.Int).Mul(amounts[i], big.NewInt(997))
		numerator := new(big.Int).Mul(amountInWithFee, reserveOut)
		denominator := new(big.Int).Add(
			new(big.Int).Mul(reserveIn, big.NewInt(1000)),
			amountInWithFee,
		)

		amounts[i+1] = new(big.Int).Div(numerator, denominator)
	}

	return amounts, nil
}

// simulateVictimTransaction 使用真实前导交易数据模拟其对池状态的影响
func (bot *MEVBot) simulateVictimTransaction(_ context.Context, initialReserves []*PairReserves, victimSwapInfo *SwapInfo) ([]*PairReserves, error) {
	updatedReserves := make([]*PairReserves, len(initialReserves))
	for i, reserve := range initialReserves {
		updatedReserves[i] = &PairReserves{
			TokenA:   reserve.TokenA,
			TokenB:   reserve.TokenB,
			ReserveA: new(big.Int).Set(reserve.ReserveA),
			ReserveB: new(big.Int).Set(reserve.ReserveB),
		}
	}

	victimPath := victimSwapInfo.Path
	var amountIn *big.Int

	switch victimSwapInfo.Function {
	case "swapExactETHForTokens", "swapExactTokensForETH", "swapExactTokensForTokens":
		amountIn = new(big.Int).Set(victimSwapInfo.AmountIn)
		bot.logger.Debug("Victim transaction using exact input: %s", amountIn.String())
	case "swapETHForExactTokens", "swapTokensForExactETH", "swapTokensForExactTokens":
		exactOutput := victimSwapInfo.AmountOutMin

		amounts, err := bot.getAmountsInWithReserves(initialReserves, victimPath, exactOutput)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate required input amount: %v", err)
		}

		amountIn = amounts[0]
		bot.logger.Debug("Victim transaction requires input: %s for exact output: %s",
			amountIn.String(), exactOutput.String())

	default:
		return nil, fmt.Errorf("unsupported swap function: %s", victimSwapInfo.Function)
	}
	pathReserveIndices, err := bot.matchReservesToPath(updatedReserves, victimPath)
	if err != nil {
		return nil, fmt.Errorf("failed to match reserves to path: %v", err)
	}
	remainingAmountIn := new(big.Int).Set(amountIn)

	for i := 0; i < len(victimPath)-1; i++ {
		tokenIn := victimPath[i]
		tokenOut := victimPath[i+1]
		reserveIndex := pathReserveIndices[i]
		pair := updatedReserves[reserveIndex]

		var reserveIn, reserveOut *big.Int
		var isReversed bool

		if pair.TokenA == tokenIn && pair.TokenB == tokenOut {
			reserveIn = pair.ReserveA
			reserveOut = pair.ReserveB
			isReversed = false
			bot.logger.Debug("Pair %s-%s in normal order", tokenIn.Hex(), tokenOut.Hex())
		} else if pair.TokenA == tokenOut && pair.TokenB == tokenIn {
			reserveIn = pair.ReserveB
			reserveOut = pair.ReserveA
			isReversed = true
			bot.logger.Debug("Pair %s-%s in reverse order", tokenIn.Hex(), tokenOut.Hex())
		} else {
			return nil, fmt.Errorf("pair tokens do not match path at index %d", i)
		}

		if reserveIn.Cmp(big.NewInt(0)) == 0 || reserveOut.Cmp(big.NewInt(0)) == 0 {
			return nil, fmt.Errorf("zero reserves in pair at index %d", i)
		}

		var amountOut *big.Int

		// 考虑0.3%的交易手续费
		amountInWithFee := new(big.Int).Mul(remainingAmountIn, big.NewInt(997))
		numerator := new(big.Int).Mul(amountInWithFee, reserveOut)

		denominator := new(big.Int).Add(
			new(big.Int).Mul(reserveIn, big.NewInt(1000)),
			amountInWithFee,
		)

		amountOut = new(big.Int).Div(numerator, denominator)
		bot.logger.Debug("Swap step %d: %s %s -> %s %s",
			i, remainingAmountIn.String(), tokenIn.Hex(), amountOut.String(), tokenOut.Hex())

		// 更新储备
		if !isReversed {
			pair.ReserveA = new(big.Int).Add(pair.ReserveA, remainingAmountIn)
			pair.ReserveB = new(big.Int).Sub(pair.ReserveB, amountOut)
			bot.logger.Debug("Updated reserves: %s %s, %s %s",
				"ReserveA", pair.ReserveA.String(), "ReserveB", pair.ReserveB.String())
		} else {
			pair.ReserveB = new(big.Int).Add(pair.ReserveB, remainingAmountIn)
			pair.ReserveA = new(big.Int).Sub(pair.ReserveA, amountOut)
			bot.logger.Debug("Updated reserves: %s %s, %s %s",
				"ReserveB", pair.ReserveB.String(), "ReserveA", pair.ReserveA.String())
		}

		// 特殊处理最后一步的输出金额
		if i == len(victimPath)-2 &&
			(victimSwapInfo.Function == "swapETHForExactTokens" ||
				victimSwapInfo.Function == "swapTokensForExactETH" ||
				victimSwapInfo.Function == "swapTokensForExactTokens") {
			// 对于精确输出交易，最后一步确保精确输出
			exactOutput := victimSwapInfo.AmountOutMin

			// 检查计算的输出是否足够
			if amountOut.Cmp(exactOutput) < 0 {
				return nil, fmt.Errorf("calculated output %s less than required exact output %s",
					amountOut.String(), exactOutput.String())
			}

			// 使用精确输出调整最终储备
			outputDiff := new(big.Int).Sub(amountOut, exactOutput)

			if !isReversed {
				// 返还未使用的输出到储备B
				pair.ReserveB = new(big.Int).Add(pair.ReserveB, outputDiff)
				bot.logger.Debug("Adjusted final ReserveB for exact output: +%s", outputDiff.String())
			} else {
				// 返还未使用的输出到储备A
				pair.ReserveA = new(big.Int).Add(pair.ReserveA, outputDiff)
				bot.logger.Debug("Adjusted final ReserveA for exact output: +%s", outputDiff.String())
			}

			// 调整输出金额为精确要求
			amountOut = exactOutput
		}

		// 为下一步准备输入金额
		remainingAmountIn = amountOut
	}

	//bot.verifyKValueChanges(initialReserves, updatedReserves)

	return updatedReserves, nil
}

// matchReservesToPath 找出路径上每个交易对对应的储备索引
func (bot *MEVBot) matchReservesToPath(reserves []*PairReserves, path []common.Address) ([]int, error) {
	if len(path) < 2 {
		return nil, fmt.Errorf("path must contain at least 2 tokens")
	}

	indices := make([]int, len(path)-1)

	for i := 0; i < len(path)-1; i++ {
		tokenA := path[i]
		tokenB := path[i+1]
		found := false

		for j, reserve := range reserves {
			if (reserve.TokenA == tokenA && reserve.TokenB == tokenB) ||
				(reserve.TokenA == tokenB && reserve.TokenB == tokenA) {
				indices[i] = j
				found = true
				break
			}
		}

		if !found {
			return nil, fmt.Errorf("no matching reserve found for tokens %s-%s in path",
				tokenA.Hex(), tokenB.Hex())
		}
	}

	return indices, nil
}

// getAmountsInWithReserves 计算要获得特定输出金额所需的输入金额
func (bot *MEVBot) getAmountsInWithReserves(reserves []*PairReserves, path []common.Address, amountOut *big.Int) ([]*big.Int, error) {
	if len(path) < 2 {
		return nil, fmt.Errorf("path must contain at least 2 tokens")
	}

	amounts := make([]*big.Int, len(path))
	amounts[len(amounts)-1] = amountOut

	for i := len(path) - 1; i > 0; i-- {
		tokenIn := path[i-1]
		tokenOut := path[i]

		var reserveIn, reserveOut *big.Int
		var found bool

		for _, reserve := range reserves {
			if reserve.TokenA == tokenIn && reserve.TokenB == tokenOut {
				reserveIn = reserve.ReserveA
				reserveOut = reserve.ReserveB
				found = true
				break
			} else if reserve.TokenA == tokenOut && reserve.TokenB == tokenIn {
				reserveIn = reserve.ReserveB
				reserveOut = reserve.ReserveA
				found = true
				break
			}
		}

		if !found {
			return nil, fmt.Errorf("no matching reserve found for tokens %s-%s",
				tokenIn.Hex(), tokenOut.Hex())
		}

		// amountIn = (reserveIn * amountOut * 1000) / ((reserveOut - amountOut) * 997)
		if amountOut.Cmp(reserveOut) >= 0 {
			return nil, fmt.Errorf("insufficient liquidity for amount out")
		}

		numerator := new(big.Int).Mul(reserveIn, amountOut)
		numerator = new(big.Int).Mul(numerator, big.NewInt(1000))

		denominator := new(big.Int).Sub(reserveOut, amountOut)
		denominator = new(big.Int).Mul(denominator, big.NewInt(997))

		amountIn := new(big.Int).Div(numerator, denominator)
		amountIn = new(big.Int).Add(amountIn, big.NewInt(1))

		amounts[i-1] = amountIn
		amountOut = amountIn
	}

	return amounts, nil
}

// verifyKValueChanges 验证并记录K值变化
func (bot *MEVBot) verifyKValueChanges(initialReserves, updatedReserves []*PairReserves) {
	if len(initialReserves) != len(updatedReserves) {
		bot.logger.Warning("Cannot verify K values: reserve arrays have different lengths")
		return
	}

	for i := 0; i < len(initialReserves); i++ {
		initial := initialReserves[i]
		updated := updatedReserves[i]

		if initial.TokenA != updated.TokenA || initial.TokenB != updated.TokenB {
			bot.logger.Warning("Cannot verify K value for pair %d: token mismatch", i)
			continue
		}

		initialK := new(big.Int).Mul(initial.ReserveA, initial.ReserveB)
		updatedK := new(big.Int).Mul(updated.ReserveA, updated.ReserveB)

		// 计算变化百分比
		if initialK.Cmp(big.NewInt(0)) > 0 {
			// (updatedK - initialK) * 10000 / initialK 得到以基点(0.01%)表示的变化
			diff := new(big.Int).Sub(updatedK, initialK)
			diffBasisPoints := new(big.Int).Mul(diff, big.NewInt(10000))
			diffBasisPoints = new(big.Int).Div(diffBasisPoints, initialK)

			bot.logger.Debug("Pair %s-%s K value change: %s basis points (%s -> %s)",
				initial.TokenA.Hex(), initial.TokenB.Hex(),
				diffBasisPoints.String(), initialK.String(), updatedK.String())

			// 检查K值是否增加了预期的约0.3%
			// 实际上由于取整和精度问题，可能略有不同
			if diffBasisPoints.Cmp(big.NewInt(25)) < 0 || diffBasisPoints.Cmp(big.NewInt(35)) > 0 {
				bot.logger.Warning("Unusual K value change: %s basis points (expected ~30)",
					diffBasisPoints.String())
			}
		}
	}
}

// getTokenValueInETH gets the value of a token in ETH
func (bot *MEVBot) getTokenValueInETH(ctx context.Context, tokenAddress common.Address, amount *big.Int) (*big.Int, error) {
	// If the token is WBNB, directly return the amount
	if tokenAddress == WBNB {
		return amount, nil
	}

	// 1. Attempt to get the value via a direct pool
	directValue, err := bot.getValueViaDirectPool(ctx, tokenAddress, WBNB, amount)
	if err == nil {
		bot.logger.Info("Token value via direct pool: %s", directValue.String())
		return directValue, nil
	}

	// 2. If direct pool fails, try using stablecoins as intermediaries
	for _, stablecoin := range stablecoins {
		indirectValue, err := bot.getValueViaIndirectPool(ctx, tokenAddress, stablecoin, WBNB, amount)
		if err == nil {
			bot.logger.Info("Token value via indirect pool (stablecoin): %s", indirectValue.String())
			return indirectValue, nil
		}
	}

	// 3. Finally, attempt to query directly using the Router
	return bot.getValueViaRouter(ctx, tokenAddress, WBNB, amount)
}

// getValueViaRouter gets token value using the router's getAmountsOut function
func (bot *MEVBot) getValueViaRouter(ctx context.Context, tokenIn, tokenOut common.Address, amountIn *big.Int) (*big.Int, error) {
	// Define the ABI for the getAmountsOut function
	routerABI := `[{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsOut","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"}]`

	// Parse the ABI
	parsedABI, err := abi.JSON(strings.NewReader(routerABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse router ABI: %v", err)
	}

	// Create the path for the token swap
	path := []common.Address{tokenIn, tokenOut}

	// Pack the call data for the getAmountsOut function
	callData, err := parsedABI.Pack("getAmountsOut", amountIn, path)
	if err != nil {
		return nil, fmt.Errorf("failed to pack getAmountsOut call: %v", err)
	}

	// Call the contract
	result, err := bot.client.CallContract(ctx, ethereum.CallMsg{To: &PancakeRouterV2, Data: callData}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to call router contract: %v", err)
	}

	// Unpack the result to get the output amounts
	var amounts []*big.Int
	err = parsedABI.UnpackIntoInterface(&amounts, "getAmountsOut", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack getAmountsOut result: %v", err)
	}

	// Return the final output amount
	return amounts[len(amounts)-1], nil
}

// getValueViaDirectPool gets token value using a direct liquidity pool
func (bot *MEVBot) getValueViaDirectPool(ctx context.Context, tokenA, tokenB common.Address, amount *big.Int) (*big.Int, error) {
	// Fetch the pair address
	pairAddress, err := bot.getPairAddress(ctx, PancakeFactoryV2, tokenA, tokenB)
	if err != nil {
		return nil, fmt.Errorf("failed to get pair address: %v", err)
	}

	// Check if the pair exists
	if pairAddress == (common.Address{}) || bytes.Equal(pairAddress.Bytes(), make([]byte, 20)) {
		return nil, fmt.Errorf("pair does not exist for tokens %s and %s", tokenA.Hex(), tokenB.Hex())
	}

	// Retrieve reserves
	reserve0, reserve1, err := bot.getReservesFromPair(ctx, pairAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get reserves: %v", err)
	}

	// Ensure reserves are valid
	if reserve0.Cmp(big.NewInt(0)) == 0 || reserve1.Cmp(big.NewInt(0)) == 0 {
		return nil, fmt.Errorf("zero reserves in pair %s", pairAddress.Hex())
	}

	// Determine the correct reserve order
	token0, err := bot.getToken0(ctx, pairAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get token0: %v", err)
	}

	var reserveA, reserveB *big.Int
	if token0 == tokenA {
		reserveA = reserve0
		reserveB = reserve1
	} else {
		reserveA = reserve1
		reserveB = reserve0
	}

	// Calculate the exchange rate and value
	exchangeRate := new(big.Float).Quo(new(big.Float).SetInt(reserveB), new(big.Float).SetInt(reserveA))
	value := new(big.Float).Mul(new(big.Float).SetInt(amount), exchangeRate)

	// Convert to big.Int
	valueInt, _ := value.Int(nil)
	return valueInt, nil
}

// getValueViaIndirectPool gets token value using an intermediate token
func (bot *MEVBot) getValueViaIndirectPool(ctx context.Context, tokenA, tokenInter, tokenB common.Address, amount *big.Int) (*big.Int, error) {
	// Step 1: tokenA → tokenInter
	reserveA, reserveInter, err := bot.getReservesOrdered(ctx, tokenA, tokenInter)
	if err != nil {
		return nil, fmt.Errorf("failed to get tokenA-tokenInter reserves: %v", err)
	}
	if reserveA.Cmp(big.NewInt(0)) == 0 || reserveInter.Cmp(big.NewInt(0)) == 0 {
		return nil, fmt.Errorf("zero reserves in tokenA-tokenInter pair")
	}

	// Step 2: tokenInter → tokenB
	reserveInter2, reserveB, err := bot.getReservesOrdered(ctx, tokenInter, tokenB)
	if err != nil {
		return nil, fmt.Errorf("failed to get tokenInter-tokenB reserves: %v", err)
	}
	if reserveInter2.Cmp(big.NewInt(0)) == 0 || reserveB.Cmp(big.NewInt(0)) == 0 {
		return nil, fmt.Errorf("zero reserves in tokenInter-tokenB pair")
	}

	// (reserveInter/reserveA) * (reserveB/reserveInter2)
	rate1 := new(big.Float).Quo(new(big.Float).SetInt(reserveInter), new(big.Float).SetInt(reserveA))
	rate2 := new(big.Float).Quo(new(big.Float).SetInt(reserveB), new(big.Float).SetInt(reserveInter2))
	combinedRate := new(big.Float).Mul(rate1, rate2)

	// Calculate final value
	value := new(big.Float).Mul(new(big.Float).SetInt(amount), combinedRate)
	valueInt, _ := value.Int(nil)
	return valueInt, nil
}

// getReservesOrdered fetches reserves for a token pair in the correct order
func (bot *MEVBot) getReservesOrdered(ctx context.Context, tokenA, tokenB common.Address) (*big.Int, *big.Int, error) {
	pairAddress, err := bot.getPairAddress(ctx, PancakeFactoryV2, tokenA, tokenB)
	if err != nil {
		return nil, nil, err
	}
	if pairAddress == (common.Address{}) || bytes.Equal(pairAddress.Bytes(), make([]byte, 20)) {
		return nil, nil, fmt.Errorf("pair does not exist for tokens %s and %s", tokenA.Hex(), tokenB.Hex())
	}

	reserve0, reserve1, err := bot.getReservesFromPair(ctx, pairAddress)
	if err != nil {
		return nil, nil, err
	}

	token0, err := bot.getToken0(ctx, pairAddress)
	if err != nil {
		return nil, nil, err
	}

	if token0 == tokenA {
		return reserve0, reserve1, nil
	} else {
		return reserve1, reserve0, nil
	}
}

// getReservesFromPair retrieves reserves from a pair contract
func (bot *MEVBot) getReservesFromPair(ctx context.Context, pairAddress common.Address) (*big.Int, *big.Int, error) {
	pairABI := `[{"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"stateMutability":"view","type":"function"}]`
	parsedABI, err := abi.JSON(strings.NewReader(pairABI))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse pair ABI: %v", err)
	}

	callData, err := parsedABI.Pack("getReserves")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to pack getReserves call: %v", err)
	}

	result, err := bot.client.CallContract(ctx, ethereum.CallMsg{To: &pairAddress, Data: callData}, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to call getReserves: %v", err)
	}

	var reserve0, reserve1 *big.Int
	var blockTimestampLast uint32
	outputs := []interface{}{&reserve0, &reserve1, &blockTimestampLast}
	err = parsedABI.UnpackIntoInterface(&outputs, "getReserves", result)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unpack getReserves result: %v", err)
	}

	return reserve0, reserve1, nil
}

// getToken0 retrieves the token0 address of a pair contract
func (bot *MEVBot) getToken0(ctx context.Context, pairAddress common.Address) (common.Address, error) {
	pairABI := `[{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`
	parsedABI, err := abi.JSON(strings.NewReader(pairABI))
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to parse pair ABI: %v", err)
	}

	callData, err := parsedABI.Pack("token0")
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to pack token0 call: %v", err)
	}

	result, err := bot.client.CallContract(ctx, ethereum.CallMsg{To: &pairAddress, Data: callData}, nil)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to call token0: %v", err)
	}

	var token0 common.Address
	err = parsedABI.UnpackIntoInterface(&token0, "token0", result)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to unpack token0 result: %v", err)
	}

	return token0, nil
}

// getPairAddress retrieves the pair address for two tokens from the factory contract
func (bot *MEVBot) getPairAddress(ctx context.Context, factoryAddress, tokenA, tokenB common.Address) (common.Address, error) {
	if bytes.Compare(tokenA.Bytes(), tokenB.Bytes()) > 0 {
		tokenA, tokenB = tokenB, tokenA
	}

	factoryABI := `[{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"}],"name":"getPair","outputs":[{"internalType":"address","name":"pair","type":"address"}],"stateMutability":"view","type":"function"}]`
	parsedABI, err := abi.JSON(strings.NewReader(factoryABI))
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to parse factory ABI: %v", err)
	}

	callData, err := parsedABI.Pack("getPair", tokenA, tokenB)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to pack getPair call: %v", err)
	}

	result, err := bot.client.CallContract(ctx, ethereum.CallMsg{To: &factoryAddress, Data: callData}, nil)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to call getPair: %v", err)
	}

	var pairAddress common.Address
	err = parsedABI.UnpackIntoInterface(&pairAddress, "getPair", result)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to unpack getPair result: %v", err)
	}

	return pairAddress, nil
}

// calculateGasCost estimates the gas cost for a backrun transaction
func (bot *MEVBot) calculateGasCost() *big.Int {
	// Default gas limit
	gasLimit := bot.config.GasLimit

	// Get current gas price
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gasPrice, err := bot.client.SuggestGasPrice(ctx)
	if err != nil {
		// If can't get current gas price, use default
		gasPrice = big.NewInt(5000000000) // 5 Gwei
	}

	// Apply multiplier
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(bot.config.GasPriceMultiplier))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(100))

	// Respect max gas price
	if gasPrice.Cmp(bot.config.MaxGasPrice) > 0 {
		gasPrice = new(big.Int).Set(bot.config.MaxGasPrice)
	}

	// Calculate total gas cost
	gasCost := new(big.Int).Mul(big.NewInt(int64(gasLimit)), gasPrice)

	return gasCost
}
