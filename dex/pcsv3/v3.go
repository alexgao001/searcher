package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
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

// Config holds all configuration parameters
type Config struct {
	BlockRazorRpcURL             string
	NodeURL                      string
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
}

// PancakeSwap V3 addresses and method signatures
var (
	// Native token (BNB on BSC)
	WBNB = common.HexToAddress("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")

	// PancakeSwap V3 Router addresses
	PancakeRouterV3 = common.HexToAddress("0x13f4EA83D0bd40E75C8222255bc855a974568Dd4")

	// PancakeSwap V3 Factory
	PancakeFactoryV3 = common.HexToAddress("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865")

	// PancakeSwap V3 Quoter
	PancakeQuoterV3 = common.HexToAddress("0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997")

	// PancakeSwap V3 NonFungiblePositionManager
	PancakePositionManager = common.HexToAddress("0x46A15B0b27311cedF172AB29E4f4766fbE7F4364")

	// Standard stablecoins for indirect routes
	Stablecoins = []common.Address{
		common.HexToAddress("0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56"), // BUSD
		common.HexToAddress("0x55d398326f99059fF775485246999027B3197955"), // USDT
		common.HexToAddress("0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"), // USDC
	}

	// Common fee tiers
	FeeTiers = []uint32{500, 3000, 10000} // 0.05%, 0.3%, 1%

	// PancakeSwap V3 method selectors
	ExactInputSingle  = "0x414bf389" // exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))
	ExactInput        = "0xc04b8d59" // exactInput((bytes,address,uint256,uint256,uint256))
	ExactOutputSingle = "0xdb3e2198" // exactOutputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))
	ExactOutput       = "0xf28c0498" // exactOutput((bytes,address,uint256,uint256,uint256))
	Multicall         = "0xac9650d8" // multicall(bytes[])
)

// ABIs
const PancakeFactoryV3ABI = `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint24","name":"fee","type":"uint24"},{"indexed":true,"internalType":"int24","name":"tickSpacing","type":"int24"}],"name":"FeeAmountEnabled","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"oldOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnerChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":true,"internalType":"uint24","name":"fee","type":"uint24"},{"indexed":false,"internalType":"int24","name":"tickSpacing","type":"int24"},{"indexed":false,"internalType":"address","name":"pool","type":"address"}],"name":"PoolCreated","type":"event"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"}],"name":"createPool","outputs":[{"internalType":"address","name":"pool","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"int24","name":"tickSpacing","type":"int24"}],"name":"enableFeeAmount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint24","name":"","type":"uint24"}],"name":"feeAmountTickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"},{"internalType":"uint24","name":"","type":"uint24"}],"name":"getPool","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_owner","type":"address"}],"name":"setOwner","outputs":[],"stateMutability":"nonpayable","type":"function"}]`

const PancakePoolV3ABI = `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"Collect","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"CollectProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid1","type":"uint256"}],"name":"Flash","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextOld","type":"uint16"},{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextNew","type":"uint16"}],"name":"IncreaseObservationCardinalityNext","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"feeProtocol0Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol0New","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1New","type":"uint8"}],"name":"SetFeeProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collect","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collectProtocol","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal0X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal1X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"flash","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"}],"name":"increaseObservationCardinalityNext","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"observations","outputs":[{"internalType":"uint32","name":"blockTimestamp","type":"uint32"},{"internalType":"int56","name":"tickCumulative","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityCumulativeX128","type":"uint160"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32[]","name":"secondsAgos","type":"uint32[]"}],"name":"observe","outputs":[{"internalType":"int56[]","name":"tickCumulatives","type":"int56[]"},{"internalType":"uint160[]","name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"positions","outputs":[{"internalType":"uint128","name":"liquidity","type":"uint128"},{"internalType":"uint256","name":"feeGrowthInside0LastX128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthInside1LastX128","type":"uint256"},{"internalType":"uint128","name":"tokensOwed0","type":"uint128"},{"internalType":"uint128","name":"tokensOwed1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"protocolFees","outputs":[{"internalType":"uint128","name":"token0","type":"uint128"},{"internalType":"uint128","name":"token1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint8","name":"feeProtocol0","type":"uint8"},{"internalType":"uint8","name":"feeProtocol1","type":"uint8"}],"name":"setFeeProtocol","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"}],"name":"snapshotCumulativesInside","outputs":[{"internalType":"int56","name":"tickCumulativeInside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityInsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsInside","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"bool","name":"zeroForOne","type":"bool"},{"internalType":"int256","name":"amountSpecified","type":"int256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[{"internalType":"int256","name":"amount0","type":"int256"},{"internalType":"int256","name":"amount1","type":"int256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"int16","name":"","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"","type":"int24"}],"name":"ticks","outputs":[{"internalType":"uint128","name":"liquidityGross","type":"uint128"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},{"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsOutside","type":"uint32"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`

const PancakeRouterV3ABI = `[{"inputs":[{"internalType":"address","name":"_deployer","type":"address"},{"internalType":"address","name":"_factory","type":"address"},{"internalType":"address","name":"_WETH9","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"WETH9","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"deployer","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"bytes","name":"path","type":"bytes"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"}],"internalType":"struct ISwapRouter.ExactInputParams","name":"params","type":"tuple"}],"name":"exactInput","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct ISwapRouter.ExactInputSingleParams","name":"params","type":"tuple"}],"name":"exactInputSingle","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"bytes","name":"path","type":"bytes"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMaximum","type":"uint256"}],"internalType":"struct ISwapRouter.ExactOutputParams","name":"params","type":"tuple"}],"name":"exactOutput","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMaximum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct ISwapRouter.ExactOutputSingleParams","name":"params","type":"tuple"}],"name":"exactOutputSingle","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes[]","name":"data","type":"bytes[]"}],"name":"multicall","outputs":[{"internalType":"bytes[]","name":"results","type":"bytes[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"int256","name":"amount0Delta","type":"int256"},{"internalType":"int256","name":"amount1Delta","type":"int256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"pancakeV3SwapCallback","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"refundETH","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermit","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"expiry","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermitAllowed","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"nonce","type":"uint256"},{"internalType":"uint256","name":"expiry","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermitAllowedIfNecessary","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"selfPermitIfNecessary","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"}],"name":"sweepToken","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"feeBips","type":"uint256"},{"internalType":"address","name":"feeRecipient","type":"address"}],"name":"sweepTokenWithFee","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"}],"name":"unwrapWETH9","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountMinimum","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"feeBips","type":"uint256"},{"internalType":"address","name":"feeRecipient","type":"address"}],"name":"unwrapWETH9WithFee","outputs":[],"stateMutability":"payable","type":"function"},{"stateMutability":"payable","type":"receive"}]`

const PancakeQuoterV3ABI = `[{"inputs":[{"internalType":"address","name":"_deployer","type":"address"},{"internalType":"address","name":"_factory","type":"address"},{"internalType":"address","name":"_WETH9","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"WETH9","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"deployer","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int256","name":"amount0Delta","type":"int256"},{"internalType":"int256","name":"amount1Delta","type":"int256"},{"internalType":"bytes","name":"path","type":"bytes"}],"name":"pancakeV3SwapCallback","outputs":[],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"bytes","name":"path","type":"bytes"},{"internalType":"uint256","name":"amountIn","type":"uint256"}],"internalType":"struct IQuoterV2.QuoteExactInputParams","name":"params","type":"tuple"}],"name":"quoteExactInput","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint160[]","name":"sqrtPriceX96AfterList","type":"uint160[]"},{"internalType":"uint32[]","name":"initializedTicksCrossedList","type":"uint32[]"},{"internalType":"uint256","name":"gasEstimate","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct IQuoterV2.QuoteExactInputSingleParams","name":"params","type":"tuple"}],"name":"quoteExactInputSingle","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceX96After","type":"uint160"},{"internalType":"uint32","name":"initializedTicksCrossed","type":"uint32"},{"internalType":"uint256","name":"gasEstimate","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"bytes","name":"path","type":"bytes"},{"internalType":"uint256","name":"amountOut","type":"uint256"}],"internalType":"struct IQuoterV2.QuoteExactOutputParams","name":"params","type":"tuple"}],"name":"quoteExactOutput","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint160[]","name":"sqrtPriceX96AfterList","type":"uint160[]"},{"internalType":"uint32[]","name":"initializedTicksCrossedList","type":"uint32[]"},{"internalType":"uint256","name":"gasEstimate","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct IQuoterV2.QuoteExactOutputSingleParams","name":"params","type":"tuple"}],"name":"quoteExactOutputSingle","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceX96After","type":"uint160"},{"internalType":"uint32","name":"initializedTicksCrossed","type":"uint32"},{"internalType":"uint256","name":"gasEstimate","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]`

const ERC20ABI = `[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"},{"name":"_spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]`

// SwapInfoV3 contains decoded swap details
type SwapInfoV3 struct {
	DEX               string
	Function          string
	TargetTx          *types.Transaction
	TokenIn           common.Address
	TokenOut          common.Address
	AmountIn          *big.Int
	AmountOutMin      *big.Int
	Fee               uint32
	Recipient         common.Address
	Deadline          *big.Int
	Path              []PathElement
	SqrtPriceLimitX96 *big.Int
}

// PathElement represents a single hop in a multi-hop path
type PathElement struct {
	TokenIn  common.Address
	TokenOut common.Address
	Fee      uint32
}

// BackrunBundle represents the bundle to be submitted to BlockRazor
type BackrunBundle struct {
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
}

// Logger provides custom logging with levels
type Logger struct {
	level LogLevel
}

// Create a new logger with specified log level
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
		log.Printf("[INFO] "+format, args...)
	}
}

func (l *Logger) Warning(format string, args ...interface{}) {
	if l.level <= WARNING {
		log.Printf("[WARNING] "+format, args...)
	}
}

func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= ERROR {
		log.Printf("[ERROR] "+format, args...)
	}
}

// Global logger instance
var logger *Logger

func main() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found")
	}

	// Initialize configuration
	config := loadConfig()

	// Initialize logger
	logger = NewLogger(config.LogLevel)
	logger.Info("Starting PancakeSwap V3 MEV bot...")

	// Setup Ethereum client
	client, err := ethclient.Dial(config.NodeURL)
	if err != nil {
		logger.Error("Failed to connect to BSC node: %v", err)
		return
	}

	// Verify ChainID matches the node
	nodeChainID, err := client.ChainID(context.Background())
	if err != nil {
		logger.Error("Failed to get ChainID from node: %v", err)
		return
	}
	if nodeChainID.Int64() != config.ChainID {
		logger.Error("ChainID mismatch: config has %d, node has %d", config.ChainID, nodeChainID.Int64())
		return
	}

	// Setup private key for sending transactions
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(config.PrivateKey, "0x"))
	if err != nil {
		logger.Error("Failed to parse private key: %v", err)
		return
	}

	// Derive the public key and address
	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		logger.Error("Error casting public key to ECDSA")
		return
	}

	address := crypto.PubkeyToAddress(*publicKeyECDSA)
	logger.Info("Using wallet address: %s", address.Hex())

	// Check wallet balance
	balance, err := client.BalanceAt(context.Background(), address, nil)
	if err != nil {
		logger.Error("Failed to get wallet balance: %v", err)
		return
	}
	logger.Info("Wallet balance: %s BNB", formatEthValue(balance))

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start monitoring transaction pool
	logger.Info("Starting to monitor the transaction pool...")
	go monitorTxPool(ctx, client, privateKey, config)

	// Handle interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	logger.Info("Shutting down gracefully...")
	cancel()
	time.Sleep(time.Second) // Give goroutines time to exit
}

func loadConfig() Config {
	// Parse router addresses from environment
	routerAddressesStr := os.Getenv("ROUTER_ADDRESSES_V3")
	if routerAddressesStr == "" {
		// Default to PancakeSwap V3 Router
		routerAddressesStr = "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4"
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
	blockRazorRpcURL := os.Getenv("BLOCKRAZOR_RPC_URL")
	if blockRazorRpcURL == "" {
		blockRazorRpcURL = "https://bsc.blockrazor.xyz"
	}

	// Parse polling interval
	pollIntervalStr := os.Getenv("POLL_INTERVAL")
	pollInterval := 500 * time.Millisecond // Default to 500ms
	if pollIntervalStr != "" {
		if interval, err := time.ParseDuration(pollIntervalStr); err == nil {
			pollInterval = interval
		}
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

	// Parse gas limit - V3 needs higher gas limit
	gasLimitStr := os.Getenv("GAS_LIMIT")
	gasLimit := uint64(700000) // Default higher for V3
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

	return Config{
		BlockRazorRpcURL:             blockRazorRpcURL,
		NodeURL:                      os.Getenv("NODE_URL"),
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
	}
}

// monitorTxPool polls the transaction pool for new transactions
func monitorTxPool(ctx context.Context, client *ethclient.Client, privateKey *ecdsa.PrivateKey, config Config) {
	logger.Info("Started transaction pool monitoring")

	// Keep track of transactions we've already seen
	seenTxs := make(map[string]bool)
	var seenTxsMutex sync.Mutex

	// Periodically clean up old transactions to prevent memory leak
	go func() {
		cleanupTicker := time.NewTicker(10 * time.Minute)
		defer cleanupTicker.Stop()

		for {
			select {
			case <-cleanupTicker.C:
				seenTxsMutex.Lock()
				logger.Debug("Cleaning up seen transactions map, current size: %d", len(seenTxs))
				// If we have too many transactions, reset the map
				if len(seenTxs) > 10000 {
					seenTxs = make(map[string]bool)
					logger.Info("Reset seen transactions map (exceeded 10000 entries)")
				}
				seenTxsMutex.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	ticker := time.NewTicker(config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Transaction pool monitoring stopped due to context cancellation")
			return
		case <-ticker.C:
			// Get pending transactions
			txPool, err := getTxPoolContent(config.NodeURL)
			if err != nil {
				logger.Warning("Error getting tx pool content: %v", err)
				continue
			}

			pendingCount := 0
			for _, txMap := range txPool.Pending {
				pendingCount += len(txMap)
			}
			logger.Debug("Found %d pending transactions", pendingCount)

			for from, nonceTxMap := range txPool.Pending {
				for _, tx := range nonceTxMap {
					// Skip if transaction has no 'to' address
					if tx.To == "" {
						continue
					}

					// Skip if transaction is not to a router we're monitoring
					if !config.RouterAddresses[strings.ToLower(tx.To)] {
						continue
					}

					// Check if we've already seen this transaction
					seenTxsMutex.Lock()
					if seenTxs[tx.Hash] {
						seenTxsMutex.Unlock()
						continue
					}
					seenTxs[tx.Hash] = true
					seenTxsMutex.Unlock()

					// Check if it's a PancakeSwap V3 function
					if len(tx.Input) < 10 {
						continue
					}
					functionSelector := tx.Input[:10]
					if !isPancakeSwapV3Function(functionSelector) {
						continue
					}

					logger.Info("Found potential PancakeSwap V3 transaction: %s from %s", tx.Hash, from)

					// Get full transaction details
					ethTx, _, err := getTransactionByHash(client, tx.Hash)
					if err != nil {
						logger.Warning("Error getting transaction %s: %v", tx.Hash, err)
						continue
					}

					// Process transaction asynchronously
					go processPendingTx(ethTx, client, privateKey, config)
				}
			}
		}
	}
}

// getTxPoolContent uses JSON-RPC to get the transaction pool content
func getTxPoolContent(nodeURL string) (*TxPoolContent, error) {
	// Create the JSON-RPC request
	jsonRpcReq := JsonRpcRequest{
		JsonRpc: "2.0",
		Method:  "txpool_content",
		Params:  []interface{}{},
		ID:      1,
	}

	// Convert to JSON
	jsonData, err := json.Marshal(jsonRpcReq)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON-RPC request: %v", err)
	}

	// Send the request
	resp, err := http.Post(
		nodeURL,
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return nil, fmt.Errorf("error sending JSON-RPC request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	// Parse the response
	var jsonRpcResp JsonRpcResponse
	if err := json.Unmarshal(body, &jsonRpcResp); err != nil {
		return nil, fmt.Errorf("error parsing JSON-RPC response: %v", err)
	}

	// Check for errors
	if jsonRpcResp.Error != nil {
		return nil, fmt.Errorf("JSON-RPC error: %v", jsonRpcResp.Error)
	}

	// Convert the result to TxPoolContent
	var txPoolContent TxPoolContent
	resultBytes, err := json.Marshal(jsonRpcResp.Result)
	if err != nil {
		return nil, fmt.Errorf("error marshaling result: %v", err)
	}

	if err := json.Unmarshal(resultBytes, &txPoolContent); err != nil {
		return nil, fmt.Errorf("error parsing tx pool content: %v", err)
	}

	return &txPoolContent, nil
}

// getTransactionByHash gets a transaction by its hash
func getTransactionByHash(client *ethclient.Client, txHash string) (*types.Transaction, bool, error) {
	hash := common.HexToHash(txHash)
	return client.TransactionByHash(context.Background(), hash)
}

// isPancakeSwapV3Function checks if a function selector is for PancakeSwap V3 swap functions
func isPancakeSwapV3Function(functionSelector string) bool {
	// List of PancakeSwap V3 swap function selectors
	swapSelectors := map[string]bool{
		ExactInputSingle:  true,
		ExactInput:        true,
		ExactOutputSingle: true,
		ExactOutput:       true,
		Multicall:         true, // multicall(bytes[]) is also commonly used for swaps
	}

	return swapSelectors[functionSelector]
}

func processPendingTx(tx *types.Transaction, client *ethclient.Client, privateKey *ecdsa.PrivateKey, config Config) {
	txHash := tx.Hash().Hex()
	logger.Info("Processing transaction: %s", txHash)

	// Try to decode the swap transaction
	swapInfo, err := decodePancakeSwapV3(tx, client)
	if err != nil {
		logger.Warning("Failed to decode swap for transaction %s: %v", txHash, err)
		return
	}

	logger.Info("Decoded %s swap via PancakeSwap V3", swapInfo.Function)

	// Print path information
	if len(swapInfo.Path) > 0 {
		for i, elem := range swapInfo.Path {
			logger.Info("  Path[%d]: %s -> %s (fee: %d)",
				i,
				formatAddressOrToken(elem.TokenIn, client),
				formatAddressOrToken(elem.TokenOut, client),
				elem.Fee)
		}
	} else {
		// For exactInputSingle/exactOutputSingle
		logger.Info("  Single Swap: %s -> %s (fee: %d)",
			formatAddressOrToken(swapInfo.TokenIn, client),
			formatAddressOrToken(swapInfo.TokenOut, client),
			swapInfo.Fee)
	}

	// Calculate optimal backrun amount and estimate profitability
	backrunInput, profit, err := findOptimalBackrunAmountV3(client, swapInfo, config)
	if err != nil {
		logger.Warning("Error calculating optimal backrun: %v", err)
		return
	}

	if backrunInput == nil || profit.Cmp(config.MinProfitThreshold) <= 0 {
		logger.Info("Not profitable enough to backrun tx %s. Expected profit: %s BNB, minimum: %s BNB",
			txHash, formatEthValue(profit), formatEthValue(config.MinProfitThreshold))
		return
	}

	logger.Info("Found profitable backrun opportunity for tx %s. Expected profit: %s BNB",
		txHash, formatEthValue(profit))

	// Create and submit backrun bundle
	err = createAndSubmitBackrunBundleV3(client, privateKey, swapInfo, backrunInput, profit, config)
	if err != nil {
		logger.Error("Failed to create/submit backrun bundle for tx %s: %v", txHash, err)
		return
	}

	logger.Info("Successfully submitted backrun bundle for tx %s", txHash)
}

func decodePancakeSwapV3(tx *types.Transaction, client *ethclient.Client) (*SwapInfoV3, error) {
	// Check if transaction is to PancakeSwap V3 router
	if tx.To() == nil || *tx.To() != PancakeRouterV3 {
		return nil, fmt.Errorf("transaction not sent to PancakeSwap V3 router")
	}

	// Get function selector from input data
	data := tx.Data()
	if len(data) < 4 {
		return nil, fmt.Errorf("transaction data too short")
	}
	selector := hexutil.Encode(data[:4])

	// Create SwapInfo struct
	swapInfo := &SwapInfoV3{
		DEX:      "PancakeSwapV3",
		TargetTx: tx,
	}

	// Special handling for multicall which could contain swap functions
	if selector == Multicall {
		return decodeMulticall(tx, client)
	}

	// Parse arguments based on function type
	switch selector {
	case ExactInputSingle:
		// exactInputSingle((address tokenIn, address tokenOut, uint24 fee, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum, uint160 sqrtPriceLimitX96))
		swapInfo.Function = "exactInputSingle"

		// We need to manually decode the struct data
		if len(data) < 4+32 {
			return nil, fmt.Errorf("data too short for exactInputSingle")
		}

		// Skip function selector and locate struct data
		structData := data[4:]

		// The data follows the memory layout of a struct in Solidity, which is a bit complex
		// We need to extract each field from the packed data

		// First 32 bytes is offset to struct
		offset := new(big.Int).SetBytes(structData[:32]).Uint64()
		if offset >= uint64(len(structData)) {
			return nil, fmt.Errorf("invalid struct offset")
		}

		structStart := structData[offset:]

		// tokenIn (20 bytes padded to 32)
		if len(structStart) < 32 {
			return nil, fmt.Errorf("data too short: cannot read tokenIn")
		}
		tokenIn := common.BytesToAddress(structStart[:32])

		// tokenOut (20 bytes padded to 32)
		if len(structStart) < 64 {
			return nil, fmt.Errorf("data too short: cannot read tokenOut")
		}
		tokenOut := common.BytesToAddress(structStart[32:64])

		// fee (uint24 padded to 32)
		if len(structStart) < 96 {
			return nil, fmt.Errorf("data too short: cannot read fee")
		}
		fee := new(big.Int).SetBytes(structStart[64:96]).Uint64()

		// recipient (20 bytes padded to 32)
		if len(structStart) < 128 {
			return nil, fmt.Errorf("data too short: cannot read recipient")
		}
		recipient := common.BytesToAddress(structStart[96:128])

		// deadline (uint256)
		if len(structStart) < 160 {
			return nil, fmt.Errorf("data too short: cannot read deadline")
		}
		deadline := new(big.Int).SetBytes(structStart[128:160])

		// amountIn (uint256)
		if len(structStart) < 192 {
			return nil, fmt.Errorf("data too short: cannot read amountIn")
		}
		amountIn := new(big.Int).SetBytes(structStart[160:192])

		// amountOutMinimum (uint256)
		if len(structStart) < 224 {
			return nil, fmt.Errorf("data too short: cannot read amountOutMinimum")
		}
		amountOutMin := new(big.Int).SetBytes(structStart[192:224])

		// sqrtPriceLimitX96 (uint160 padded to 32)
		if len(structStart) < 256 {
			return nil, fmt.Errorf("data too short: cannot read sqrtPriceLimitX96")
		}
		sqrtPriceLimit := new(big.Int).SetBytes(structStart[224:256])

		swapInfo.TokenIn = tokenIn
		swapInfo.TokenOut = tokenOut
		swapInfo.Fee = uint32(fee)
		swapInfo.Recipient = recipient
		swapInfo.Deadline = deadline
		swapInfo.AmountIn = amountIn
		swapInfo.AmountOutMin = amountOutMin
		swapInfo.SqrtPriceLimitX96 = sqrtPriceLimit

	case ExactInput:
		// exactInput((bytes path, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum))
		swapInfo.Function = "exactInput"

		// Similar approach as above, but with different struct layout
		if len(data) < 4+32 {
			return nil, fmt.Errorf("data too short for exactInput")
		}

		// Skip function selector and locate struct data
		structData := data[4:]

		// Get offset to struct
		offset := new(big.Int).SetBytes(structData[:32]).Uint64()
		if offset >= uint64(len(structData)) {
			return nil, fmt.Errorf("invalid struct offset")
		}

		structStart := structData[offset:]

		// path (bytes) - this is more complex as it's dynamic
		if len(structStart) < 32 {
			return nil, fmt.Errorf("data too short: cannot read path offset")
		}
		pathOffset := new(big.Int).SetBytes(structStart[:32]).Uint64()
		if pathOffset >= uint64(len(structStart)) {
			return nil, fmt.Errorf("invalid path offset")
		}

		pathData := structStart[pathOffset:]
		if len(pathData) < 32 {
			return nil, fmt.Errorf("data too short: cannot read path length")
		}

		pathLength := new(big.Int).SetBytes(pathData[:32]).Uint64()
		if 32+pathLength > uint64(len(pathData)) {
			return nil, fmt.Errorf("invalid path length")
		}

		// Path encoding in V3 is more complex, it's a packed bytes that needs special decoding
		// Format: <tokenIn><fee><tokenOut><fee><tokenNext>...<tokenN>
		path := pathData[32 : 32+pathLength]
		swapInfo.Path = decodePancakeV3Path(path)

		if len(swapInfo.Path) > 0 {
			swapInfo.TokenIn = swapInfo.Path[0].TokenIn
			swapInfo.TokenOut = swapInfo.Path[len(swapInfo.Path)-1].TokenOut
			swapInfo.Fee = swapInfo.Path[0].Fee // Use first fee for single-hop cases
		}

		// recipient (at offset 32 from start of struct)
		if len(structStart) < 64 {
			return nil, fmt.Errorf("data too short: cannot read recipient")
		}
		recipient := common.BytesToAddress(structStart[32:64])

		// deadline (at offset 64 from start of struct)
		if len(structStart) < 96 {
			return nil, fmt.Errorf("data too short: cannot read deadline")
		}
		deadline := new(big.Int).SetBytes(structStart[64:96])

		// amountIn (at offset 96 from start of struct)
		if len(structStart) < 128 {
			return nil, fmt.Errorf("data too short: cannot read amountIn")
		}
		amountIn := new(big.Int).SetBytes(structStart[96:128])

		// amountOutMinimum (at offset 128 from start of struct)
		if len(structStart) < 160 {
			return nil, fmt.Errorf("data too short: cannot read amountOutMinimum")
		}
		amountOutMin := new(big.Int).SetBytes(structStart[128:160])

		swapInfo.Recipient = recipient
		swapInfo.Deadline = deadline
		swapInfo.AmountIn = amountIn
		swapInfo.AmountOutMin = amountOutMin

	case ExactOutputSingle:
		// exactOutputSingle((address tokenIn, address tokenOut, uint24 fee, address recipient, uint256 deadline, uint256 amountOut, uint256 amountInMaximum, uint160 sqrtPriceLimitX96))
		swapInfo.Function = "exactOutputSingle"

		// Similar to exactInputSingle but with amountOut instead of amountIn
		if len(data) < 4+32 {
			return nil, fmt.Errorf("data too short for exactOutputSingle")
		}

		// Skip function selector and locate struct data
		structData := data[4:]

		// First 32 bytes is offset to struct
		offset := new(big.Int).SetBytes(structData[:32]).Uint64()
		if offset >= uint64(len(structData)) {
			return nil, fmt.Errorf("invalid struct offset")
		}

		structStart := structData[offset:]

		// tokenIn (20 bytes padded to 32)
		if len(structStart) < 32 {
			return nil, fmt.Errorf("data too short: cannot read tokenIn")
		}
		tokenIn := common.BytesToAddress(structStart[:32])

		// tokenOut (20 bytes padded to 32)
		if len(structStart) < 64 {
			return nil, fmt.Errorf("data too short: cannot read tokenOut")
		}
		tokenOut := common.BytesToAddress(structStart[32:64])

		// fee (uint24 padded to 32)
		if len(structStart) < 96 {
			return nil, fmt.Errorf("data too short: cannot read fee")
		}
		fee := new(big.Int).SetBytes(structStart[64:96]).Uint64()

		// recipient (20 bytes padded to 32)
		if len(structStart) < 128 {
			return nil, fmt.Errorf("data too short: cannot read recipient")
		}
		recipient := common.BytesToAddress(structStart[96:128])

		// deadline (uint256)
		if len(structStart) < 160 {
			return nil, fmt.Errorf("data too short: cannot read deadline")
		}
		deadline := new(big.Int).SetBytes(structStart[128:160])

		// amountOut (uint256)
		if len(structStart) < 192 {
			return nil, fmt.Errorf("data too short: cannot read amountOut")
		}
		amountOut := new(big.Int).SetBytes(structStart[160:192])

		// amountInMaximum (uint256)
		if len(structStart) < 224 {
			return nil, fmt.Errorf("data too short: cannot read amountInMaximum")
		}
		amountInMax := new(big.Int).SetBytes(structStart[192:224])

		// sqrtPriceLimitX96 (uint160 padded to 32)
		if len(structStart) < 256 {
			return nil, fmt.Errorf("data too short: cannot read sqrtPriceLimitX96")
		}
		sqrtPriceLimit := new(big.Int).SetBytes(structStart[224:256])

		swapInfo.TokenIn = tokenIn
		swapInfo.TokenOut = tokenOut
		swapInfo.Fee = uint32(fee)
		swapInfo.Recipient = recipient
		swapInfo.Deadline = deadline
		swapInfo.AmountIn = amountInMax   // For exactOutput, this is the maximum input
		swapInfo.AmountOutMin = amountOut // For exactOutput, this is the exact output
		swapInfo.SqrtPriceLimitX96 = sqrtPriceLimit

	case ExactOutput:
		// exactOutput((bytes path, address recipient, uint256 deadline, uint256 amountOut, uint256 amountInMaximum))
		swapInfo.Function = "exactOutput"

		// Similar to exactInput but with amountOut instead of amountIn
		if len(data) < 4+32 {
			return nil, fmt.Errorf("data too short for exactOutput")
		}

		// Skip function selector and locate struct data
		structData := data[4:]

		// Get offset to struct
		offset := new(big.Int).SetBytes(structData[:32]).Uint64()
		if offset >= uint64(len(structData)) {
			return nil, fmt.Errorf("invalid struct offset")
		}

		structStart := structData[offset:]

		// path (bytes) - this is more complex as it's dynamic
		if len(structStart) < 32 {
			return nil, fmt.Errorf("data too short: cannot read path offset")
		}
		pathOffset := new(big.Int).SetBytes(structStart[:32]).Uint64()
		if pathOffset >= uint64(len(structStart)) {
			return nil, fmt.Errorf("invalid path offset")
		}

		pathData := structStart[pathOffset:]
		if len(pathData) < 32 {
			return nil, fmt.Errorf("data too short: cannot read path length")
		}

		pathLength := new(big.Int).SetBytes(pathData[:32]).Uint64()
		if 32+pathLength > uint64(len(pathData)) {
			return nil, fmt.Errorf("invalid path length")
		}

		// Path for V3 ExactOutput is in reverse (tokenOut -> tokenIn)
		path := pathData[32 : 32+pathLength]
		// For exactOutput, the path is encoded in reverse (from tokenOut to tokenIn)
		pathElements := decodePancakeV3Path(path)

		// Reverse the path elements for exactOutput
		swapInfo.Path = make([]PathElement, len(pathElements))
		for i, element := range pathElements {
			swapInfo.Path[len(pathElements)-1-i] = PathElement{
				TokenIn:  element.TokenOut,
				TokenOut: element.TokenIn,
				Fee:      element.Fee,
			}
		}

		if len(swapInfo.Path) > 0 {
			swapInfo.TokenIn = swapInfo.Path[0].TokenIn
			swapInfo.TokenOut = swapInfo.Path[len(swapInfo.Path)-1].TokenOut
			swapInfo.Fee = swapInfo.Path[0].Fee
		}

		// recipient (at offset 32 from start of struct)
		if len(structStart) < 64 {
			return nil, fmt.Errorf("data too short: cannot read recipient")
		}
		recipient := common.BytesToAddress(structStart[32:64])

		// deadline (at offset 64 from start of struct)
		if len(structStart) < 96 {
			return nil, fmt.Errorf("data too short: cannot read deadline")
		}
		deadline := new(big.Int).SetBytes(structStart[64:96])

		// amountOut (at offset 96 from start of struct)
		if len(structStart) < 128 {
			return nil, fmt.Errorf("data too short: cannot read amountOut")
		}
		amountOut := new(big.Int).SetBytes(structStart[96:128])

		// amountInMaximum (at offset 128 from start of struct)
		if len(structStart) < 160 {
			return nil, fmt.Errorf("data too short: cannot read amountInMaximum")
		}
		amountInMax := new(big.Int).SetBytes(structStart[128:160])

		swapInfo.Recipient = recipient
		swapInfo.Deadline = deadline
		swapInfo.AmountIn = amountInMax   // For exactOutput, this is the maximum input
		swapInfo.AmountOutMin = amountOut // For exactOutput, this is the exact output

	default:
		return nil, fmt.Errorf("unsupported V3 function selector: %s", selector)
	}

	return swapInfo, nil
}

// decodeMulticall attempts to decode multicall transactions to find swap operations
func decodeMulticall(tx *types.Transaction, client *ethclient.Client) (*SwapInfoV3, error) {
	txHash := tx.Hash().Hex()
	logger.Debug("Attempting to decode multicall transaction: %s", txHash)

	// Load router ABI
	routerABI, err := abi.JSON(strings.NewReader(PancakeRouterV3ABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse router ABI: %v", err)
	}

	data := tx.Data()
	if len(data) < 4 {
		return nil, fmt.Errorf("transaction data too short")
	}

	selector := hexutil.Encode(data[:4])
	if selector != Multicall {
		return nil, fmt.Errorf("not a multicall function: %s", selector)
	}

	data = data[4:] // Skip function selector

	multicallMethod, exists := routerABI.Methods["multicall"]
	if !exists {
		return nil, fmt.Errorf("multicall method not found in ABI")
	}

	var callDataArray [][]byte
	decodedValues, err := multicallMethod.Inputs.Unpack(data)
	if err != nil {
		logger.Warning("ABI decoding of multicall failed: %v - trying manual decoding", err)
	} else if len(decodedValues) == 1 {
		if cdArray, ok := decodedValues[0].([][]byte); ok {
			callDataArray = cdArray
		}
	}

	if callDataArray == nil {
		logger.Debug("Attempting manual multicall decoding")
		if len(data) < 32 {
			return nil, fmt.Errorf("data too short for array offset")
		}
		arrayOffset := new(big.Int).SetBytes(data[:32]).Uint64()
		if arrayOffset >= uint64(len(data)) {
			return nil, fmt.Errorf("invalid array offset: %d in data of length %d", arrayOffset, len(data))
		}

		arrayData := data[arrayOffset:]
		if len(arrayData) < 32 {
			return nil, fmt.Errorf("data too short for array length")
		}
		arrayLength := new(big.Int).SetBytes(arrayData[:32]).Uint64()
		logger.Debug("Manual decode: found array of length %d at offset %d", arrayLength, arrayOffset)

		if arrayLength > 50 {
			return nil, fmt.Errorf("unreasonable array length: %d", arrayLength)
		}

		manualCallDataArray := make([][]byte, 0, arrayLength)
		for i := uint64(0); i < arrayLength; i++ {
			if len(arrayData) < int(32*(i+2)) {
				return nil, fmt.Errorf("data too short for element %d offset", i)
			}
			elementOffset := new(big.Int).SetBytes(arrayData[32*(i+1) : 32*(i+2)]).Uint64()
			if elementOffset >= uint64(len(arrayData)) {
				return nil, fmt.Errorf("invalid element offset: %d", elementOffset)
			}
			elementData := arrayData[elementOffset:]
			if len(elementData) < 32 {
				return nil, fmt.Errorf("data too short for element %d length", i)
			}
			elementLength := new(big.Int).SetBytes(elementData[:32]).Uint64()
			if 32+elementLength > uint64(len(elementData)) {
				return nil, fmt.Errorf("invalid element length: %d", elementLength)
			}
			manualCallDataArray = append(manualCallDataArray, elementData[32:32+elementLength])
		}
		callDataArray = manualCallDataArray
	}

	for _, callData := range callDataArray {
		if len(callData) < 4 {
			logger.Warning("Call data too short: %x", callData)
			continue
		}
		callSelector := hexutil.Encode(callData[:4])
		if isPancakeSwapV3Function(callSelector) {
			logger.Debug("Found PancakeSwap V3 function in multicall: %s", callSelector)
			// Decode the specific swap function here
			return decodePancakeSwapV3(types.NewTx(&types.DynamicFeeTx{Data: callData}), client)
		} else {
			logger.Debug("Unknown function selector in multicall: %s", callSelector)
		}
	}

	return nil, fmt.Errorf("no swap method found in multicall")
}

// Helper function to get a friendly name for function selectors
func getFunctionName(selector string) string {
	switch selector {
	case ExactInputSingle:
		return "exactInputSingle"
	case ExactInput:
		return "exactInput"
	case ExactOutputSingle:
		return "exactOutputSingle"
	case ExactOutput:
		return "exactOutput"
	case Multicall:
		return "multicall"
	default:
		return selector
	}
}

// decodePancakeV3Path decodes the packed path bytes into a sequence of token addresses and fees
func decodePancakeV3Path(path []byte) []PathElement {
	// Path format: <tokenIn><fee><tokenOut><fee><tokenNext>...<tokenN>
	// Where each token is 20 bytes and each fee is 3 bytes
	// Need at least 20 (tokenIn) + 3 (fee) + 20 (tokenOut) = 43 bytes
	if len(path) < 43 {
		return nil
	}

	var elements []PathElement

	// First token
	tokenIn := common.BytesToAddress(path[:20])
	currentIdx := 20

	// Process each (fee, tokenOut) pair
	for currentIdx+23 <= len(path) {
		// Read fee (3 bytes)
		feeBytes := make([]byte, 4)
		copy(feeBytes[1:], path[currentIdx:currentIdx+3])
		fee := binary.BigEndian.Uint32(feeBytes)
		currentIdx += 3

		// Read next token
		if currentIdx+20 > len(path) {
			break
		}
		tokenOut := common.BytesToAddress(path[currentIdx : currentIdx+20])
		currentIdx += 20

		// Add path element
		elements = append(elements, PathElement{
			TokenIn:  tokenIn,
			TokenOut: tokenOut,
			Fee:      fee,
		})

		// Current tokenOut becomes next tokenIn
		tokenIn = tokenOut
	}

	return elements
}

// encodePancakeV3Path encodes a path into the V3 format
func encodePancakeV3Path(path []PathElement) []byte {
	if len(path) == 0 {
		return nil
	}

	// Calculate total size:
	// First token (20 bytes) + Each (fee + token) (3 + 20 bytes each)
	totalSize := 20 + (len(path) * 23)

	// Create buffer with exact size
	encodedPath := make([]byte, 0, totalSize)

	// Add first token
	encodedPath = append(encodedPath, path[0].TokenIn.Bytes()...)

	// Add each (fee, token) pair
	for _, element := range path {
		// Convert fee to 3 bytes
		feeBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(feeBytes, element.Fee)
		encodedPath = append(encodedPath, feeBytes[1:4]...) // Take only 3 bytes

		// Add token out
		encodedPath = append(encodedPath, element.TokenOut.Bytes()...)
	}

	return encodedPath
}

// quoteExactInput estimates the output amount for a given V3 swap
func quoteExactInput(client *ethclient.Client, swapInfo *SwapInfoV3) (*big.Int, error) {
	ctx := context.Background()
	quoterABI, err := abi.JSON(strings.NewReader(PancakeQuoterV3ABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse quoter ABI: %v", err)
	}

	var callData []byte

	if len(swapInfo.Path) > 0 {
		// Multi-hop path
		// Encode the path in V3 format
		encodedPath := encodePancakeV3Path(swapInfo.Path)

		// Pack quoteExactInput call
		callData, err = quoterABI.Pack("quoteExactInput",
			// Struct params: (bytes path, uint256 amountIn)
			struct {
				Path     []byte
				AmountIn *big.Int
			}{
				Path:     encodedPath,
				AmountIn: swapInfo.AmountIn,
			})
	} else {
		// Single-hop swap
		// Pack quoteExactInputSingle call
		callData, err = quoterABI.Pack("quoteExactInputSingle",
			// Struct params: (address tokenIn, address tokenOut, uint256 amountIn, uint24 fee, uint160 sqrtPriceLimitX96)
			struct {
				TokenIn           common.Address
				TokenOut          common.Address
				AmountIn          *big.Int
				Fee               uint32
				SqrtPriceLimitX96 *big.Int
			}{
				TokenIn:           swapInfo.TokenIn,
				TokenOut:          swapInfo.TokenOut,
				AmountIn:          swapInfo.AmountIn,
				Fee:               swapInfo.Fee,
				SqrtPriceLimitX96: big.NewInt(0), // sqrtPriceLimitX96 = 0 (no price limit)
			})
	}

	if err != nil {
		return nil, fmt.Errorf("failed to pack quoter call: %v", err)
	}

	// Execute call
	msg := ethereum.CallMsg{
		To:   &PancakeQuoterV3,
		Data: callData,
	}

	result, err := client.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, fmt.Errorf("quoter call failed: %v", err)
	}

	// For quoteExactInput and quoteExactInputSingle, first value is the amount out
	if len(result) < 32 {
		return nil, fmt.Errorf("result too short")
	}

	amountOut := new(big.Int).SetBytes(result[:32])
	return amountOut, nil
}

// findOptimalBackrunAmountV3 calculates the most profitable input amount for a backrun in V3
func findOptimalBackrunAmountV3(client *ethclient.Client, swapInfo *SwapInfoV3, config Config) (*big.Int, *big.Int, error) {
	logger.Info("Finding optimal backrun amount for V3 swap")

	// Create reversed path for backrun
	var reversedPath []PathElement

	if len(swapInfo.Path) > 0 {
		// For multi-hop path
		reversedPath = make([]PathElement, len(swapInfo.Path))
		for i, element := range swapInfo.Path {
			reversedPath[len(swapInfo.Path)-1-i] = PathElement{
				TokenIn:  element.TokenOut,
				TokenOut: element.TokenIn,
				Fee:      element.Fee,
			}
		}
	} else {
		// For single-hop
		reversedPath = []PathElement{
			{
				TokenIn:  swapInfo.TokenOut,
				TokenOut: swapInfo.TokenIn,
				Fee:      swapInfo.Fee,
			},
		}
	}

	// Determine the backrun strategy
	var strategy string
	var maxBackrunInput *big.Int

	// Get information about the input/output tokens
	inputIsWBNB := (swapInfo.TokenIn == WBNB)
	outputIsWBNB := (swapInfo.TokenOut == WBNB)

	// Estimate the output of the original transaction
	outputAmount, err := quoteExactInput(client, swapInfo)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to quote original transaction output: %v", err)
	}
	logger.Info("Estimated original tx output: %s", formatTokenAmount(outputAmount))

	// Determine strategy and maximum input
	if inputIsWBNB && !outputIsWBNB {
		// Original: ETH -> Token, Backrun: Token -> ETH
		strategy = "TokenToETH"
		maxBackrunInput = outputAmount
		logger.Info("Strategy: TokenToETH, Max input: %s tokens", formatTokenAmount(maxBackrunInput))
	} else if !inputIsWBNB && outputIsWBNB {
		// Original: Token -> ETH, Backrun: ETH -> Token
		strategy = "ETHToToken"
		maxBackrunInput = outputAmount
		logger.Info("Strategy: ETHToToken, Max input: %s BNB", formatEthValue(maxBackrunInput))
	} else {
		// Original: TokenA -> TokenB, Backrun: TokenB -> TokenA
		strategy = "TokenToToken"
		maxBackrunInput = outputAmount
		logger.Info("Strategy: TokenToToken, Max input: %s tokens", formatTokenAmount(maxBackrunInput))
	}

	// Test different percentages of the maximum input to find optimal amount
	percentages := []int{10, 20, 30, 40, 50, 60, 70, 80, 90}
	var bestInput, bestProfit *big.Int

	// Try each percentage of max input
	for _, percentage := range percentages {
		// Calculate input amount for this percentage
		inputAmount := new(big.Int).Mul(maxBackrunInput, big.NewInt(int64(percentage)))
		inputAmount = new(big.Int).Div(inputAmount, big.NewInt(100))

		// Skip if input amount is too small
		if inputAmount.Cmp(big.NewInt(1000)) <= 0 {
			logger.Debug("Skipping %d%% input amount (too small): %s", percentage, formatTokenAmount(inputAmount))
			continue
		}

		// Calculate expected profit for this input amount
		logger.Debug("Testing %d%% input amount: %s", percentage, formatTokenAmount(inputAmount))
		profit, err := calculateBackrunProfitV3(client, strategy, inputAmount, reversedPath, config)
		if err != nil {
			logger.Debug("Error calculating profit for %d%% input: %v", percentage, err)
			continue
		}

		logger.Debug("Profit for %d%% input: %s BNB", percentage, formatEthValue(profit))

		// Update best input if this is more profitable
		if bestProfit == nil || profit.Cmp(bestProfit) > 0 {
			bestProfit = profit
			bestInput = inputAmount
			logger.Info("New best profit: %s BNB with input: %s",
				formatEthValue(bestProfit), formatTokenAmount(bestInput))
		}
	}

	// If we found a profitable input, add a buffer to account for price changes
	if bestInput != nil && bestProfit.Cmp(big.NewInt(0)) > 0 {
		// Add 5% buffer to input amount
		bestInput = new(big.Int).Mul(bestInput, big.NewInt(105))
		bestInput = new(big.Int).Div(bestInput, big.NewInt(100))
		logger.Info("Final backrun input (with 5% buffer): %s", formatTokenAmount(bestInput))
	} else {
		logger.Info("No profitable backrun found")
	}

	return bestInput, bestProfit, nil
}

// calculateBackrunProfitV3 estimates the profit for a given backrun strategy and input amount in V3
func calculateBackrunProfitV3(client *ethclient.Client, strategy string, inputAmount *big.Int,
	path []PathElement, config Config) (*big.Int, error) {
	ctx := context.Background()

	// Quote the backrun transaction
	var outputAmount *big.Int
	var err error

	if len(path) == 1 {
		// Single-hop path
		outputAmount, err = quoteV3ExactInputSingle(ctx, client, path[0].TokenIn, path[0].TokenOut,
			path[0].Fee, inputAmount)
	} else {
		// Multi-hop path
		outputAmount, err = quoteV3ExactInputMultiHop(ctx, client, path, inputAmount)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to quote backrun output: %v", err)
	}

	// Calculate profit based on strategy
	var profit *big.Int

	switch strategy {
	case "TokenToETH":
		// Converting token to ETH, profit is ETH received minus input cost
		// First check the token's value in ETH
		inputValueInETH, err := getTokenValueInETHV3(ctx, client, path[0].TokenIn, inputAmount)
		if err != nil {
			return nil, fmt.Errorf("failed to get token value in ETH: %v", err)
		}

		// Profit is output ETH minus input value
		profit = new(big.Int).Sub(outputAmount, inputValueInETH)

	case "ETHToToken":
		// Converting ETH to token, profit is token value minus ETH spent
		outputValueInETH, err := getTokenValueInETHV3(ctx, client, path[len(path)-1].TokenOut, outputAmount)
		if err != nil {
			return nil, fmt.Errorf("failed to get token value in ETH: %v", err)
		}

		// Profit is output value minus input ETH
		profit = new(big.Int).Sub(outputValueInETH, inputAmount)

	case "TokenToToken":
		// Converting token A to token B
		inputValueInETH, err := getTokenValueInETHV3(ctx, client, path[0].TokenIn, inputAmount)
		if err != nil {
			return nil, fmt.Errorf("failed to get input token value in ETH: %v", err)
		}

		outputValueInETH, err := getTokenValueInETHV3(ctx, client, path[len(path)-1].TokenOut, outputAmount)
		if err != nil {
			return nil, fmt.Errorf("failed to get output token value in ETH: %v", err)
		}

		// Profit is output value minus input value
		profit = new(big.Int).Sub(outputValueInETH, inputValueInETH)
	}

	// Subtract estimated gas cost
	gasCost := calculateGasCost(client, config)
	profit = new(big.Int).Sub(profit, gasCost)

	return profit, nil
}

// quoteV3ExactInputSingle estimates the output of a single-hop V3 swap
func quoteV3ExactInputSingle(ctx context.Context, client *ethclient.Client,
	tokenIn, tokenOut common.Address, fee uint32,
	amountIn *big.Int) (*big.Int, error) {

	if amountIn == nil || amountIn.Cmp(big.NewInt(0)) <= 0 {
		return nil, fmt.Errorf("invalid input amount")
	}

	quoterABI, err := abi.JSON(strings.NewReader(PancakeQuoterV3ABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse quoter ABI: %v", err)
	}

	// Pack quoteExactInputSingle call
	callData, err := quoterABI.Pack("quoteExactInputSingle",
		struct {
			TokenIn           common.Address
			TokenOut          common.Address
			AmountIn          *big.Int
			Fee               uint32
			SqrtPriceLimitX96 *big.Int
		}{
			TokenIn:           tokenIn,
			TokenOut:          tokenOut,
			AmountIn:          amountIn,
			Fee:               uint32(fee),
			SqrtPriceLimitX96: big.NewInt(0), // No price limit
		})

	if err != nil {
		return nil, fmt.Errorf("failed to pack quoter call: %v", err)
	}

	// Execute call
	msg := ethereum.CallMsg{
		To:   &PancakeQuoterV3,
		Data: callData,
	}

	result, err := client.CallContract(ctx, msg, nil)
	if err != nil {
		// Useful for debugging
		logger.Debug("Quote failed for %s -> %s with fee %d: %v",
			tokenIn.Hex(),
			tokenOut.Hex(),
			fee, err)
		return nil, err
	}

	// For quoteExactInputSingle, first value is the amount out
	if len(result) < 32 {
		return nil, fmt.Errorf("quoter result too short")
	}

	amountOut := new(big.Int).SetBytes(result[:32])
	return amountOut, nil
}

// quoteV3ExactInputMultiHop estimates the output of a multi-hop V3 swap
func quoteV3ExactInputMultiHop(ctx context.Context, client *ethclient.Client,
	path []PathElement, amountIn *big.Int) (*big.Int, error) {

	if len(path) == 0 {
		return nil, fmt.Errorf("empty path")
	}

	if amountIn == nil || amountIn.Cmp(big.NewInt(0)) <= 0 {
		return nil, fmt.Errorf("invalid input amount")
	}

	quoterABI, err := abi.JSON(strings.NewReader(PancakeQuoterV3ABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse quoter ABI: %v", err)
	}

	// Encode the path
	encodedPath := encodePancakeV3Path(path)
	if len(encodedPath) == 0 {
		return nil, fmt.Errorf("failed to encode path")
	}

	// Pack quoteExactInput call
	callData, err := quoterABI.Pack("quoteExactInput",
		struct {
			Path     []byte
			AmountIn *big.Int
		}{
			Path:     encodedPath,
			AmountIn: amountIn,
		})

	if err != nil {
		return nil, fmt.Errorf("failed to pack quoter call: %v", err)
	}

	// Execute call
	msg := ethereum.CallMsg{
		To:   &PancakeQuoterV3,
		Data: callData,
	}

	result, err := client.CallContract(ctx, msg, nil)
	if err != nil {
		// Log the path for debugging
		pathStr := ""
		for i, p := range path {
			pathStr += fmt.Sprintf("%s->", p.TokenIn.Hex()[:8])
			if i == len(path)-1 {
				pathStr += p.TokenOut.Hex()[:8]
			}
		}
		logger.Debug("Multi-hop quote failed for path %s: %v", pathStr, err)
		return nil, err
	}

	// For quoteExactInput, first value is the amount out
	if len(result) < 32 {
		return nil, fmt.Errorf("quoter result too short")
	}

	amountOut := new(big.Int).SetBytes(result[:32])
	return amountOut, nil
}

// getTokenValueInETHV3 calculates the ETH value of a token amount in V3
func getTokenValueInETHV3(ctx context.Context, client *ethclient.Client,
	tokenAddress common.Address, amount *big.Int) (*big.Int, error) {

	// If token is WBNB, return amount directly
	if tokenAddress == WBNB {
		return amount, nil
	}

	// Common fee tiers to try
	feeTiers := []uint32{500, 3000, 10000} // 0.05%, 0.3%, 1%

	var lastErr error
	// Try different fee tiers
	for _, fee := range feeTiers {
		// Try direct path first
		outputAmount, err := quoteV3ExactInputSingle(ctx, client, tokenAddress, WBNB, fee, amount)
		if err == nil {
			return outputAmount, nil
		}
		lastErr = err

		// If direct path fails, try with a stablecoin as intermediary
		for _, stablecoin := range Stablecoins {
			// Try token -> stablecoin -> WBNB
			path := []PathElement{
				{TokenIn: tokenAddress, TokenOut: stablecoin, Fee: fee},
				{TokenIn: stablecoin, TokenOut: WBNB, Fee: fee},
			}

			outputAmount, err := quoteV3ExactInputMultiHop(ctx, client, path, amount)
			if err == nil {
				return outputAmount, nil
			}
		}
	}

	return nil, fmt.Errorf("failed to determine token value: %v", lastErr)
}

// calculateGasCost estimates the gas cost for a backrun transaction
func calculateGasCost(client *ethclient.Client, config Config) *big.Int {
	// Default gas limit - V3 uses more gas than V2
	gasLimit := config.GasLimit

	// Get current gas price
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		// If can't get current gas price, use default
		gasPrice = big.NewInt(5000000000) // 5 Gwei
	}

	// Apply multiplier
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(config.GasPriceMultiplier))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(100))

	// Respect max gas price
	if gasPrice.Cmp(config.MaxGasPrice) > 0 {
		gasPrice = new(big.Int).Set(config.MaxGasPrice)
	}

	// Calculate total gas cost
	gasCost := new(big.Int).Mul(big.NewInt(int64(gasLimit)), gasPrice)

	return gasCost
}

// createAndSubmitBackrunBundleV3 creates and submits a backrun bundle for V3
func createAndSubmitBackrunBundleV3(client *ethclient.Client, privateKey *ecdsa.PrivateKey,
	swapInfo *SwapInfoV3, backrunInput *big.Int,
	expectedProfit *big.Int, config Config) error {
	logger.Info("Creating backrun bundle for tx %s with expected profit: %s BNB",
		swapInfo.TargetTx.Hash().Hex(), formatEthValue(expectedProfit))

	// Get current block number
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	blockNumber, err := client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block number: %v", err)
	}

	// Get wallet address
	fromAddress := crypto.PubkeyToAddress(*privateKey.Public().(*ecdsa.PublicKey))

	// Create a reversed path for backrun
	var reversedPath []PathElement

	if len(swapInfo.Path) > 0 {
		// For multi-hop path
		reversedPath = make([]PathElement, len(swapInfo.Path))
		for i, element := range swapInfo.Path {
			reversedPath[len(swapInfo.Path)-1-i] = PathElement{
				TokenIn:  element.TokenOut,
				TokenOut: element.TokenIn,
				Fee:      element.Fee,
			}
		}
	} else {
		// For single-hop
		reversedPath = []PathElement{
			{
				TokenIn:  swapInfo.TokenOut,
				TokenOut: swapInfo.TokenIn,
				Fee:      swapInfo.Fee,
			},
		}
	}

	// Check if token approval is needed
	backrunTokenIn := reversedPath[0].TokenIn

	// We need approval if input token is not ETH/WBNB
	if backrunTokenIn != WBNB {
		approvalTx, err := checkAndApproveToken(ctx, client, privateKey, backrunTokenIn, PancakeRouterV3, backrunInput, config)
		if err != nil {
			return fmt.Errorf("token approval check failed: %v", err)
		}

		// If approval needed, send and wait for it
		if approvalTx != nil {
			logger.Info("Sending token approval transaction...")
			err = client.SendTransaction(ctx, approvalTx)
			if err != nil {
				return fmt.Errorf("failed to send approval transaction: %v", err)
			}

			// Wait for approval tx to be mined
			receipt, err := waitForTransaction(ctx, client, approvalTx.Hash(), config.WaitForApprovalConfirmations)
			if err != nil {
				return fmt.Errorf("error waiting for approval transaction: %v", err)
			}

			if receipt.Status == 0 {
				return fmt.Errorf("approval transaction failed")
			}

			logger.Info("Token approval transaction confirmed")
		}
	}

	// Create backrun transaction
	backrunTx, err := createBackrunTransactionV3(client, privateKey, backrunInput, reversedPath, config)
	if err != nil {
		return fmt.Errorf("failed to create backrun transaction: %v", err)
	}

	// Simulate transaction if enabled
	if config.SimulateTransactions {
		err = simulateTransaction(ctx, client, fromAddress, backrunTx)
		if err != nil {
			return fmt.Errorf("transaction simulation failed: %v", err)
		}
		logger.Info("Backrun transaction simulation successful")
	}

	// Get raw transactions for the bundle
	origTxRaw, err := getRawTransaction(swapInfo.TargetTx)
	if err != nil {
		return fmt.Errorf("failed to get raw original transaction: %v", err)
	}

	backrunTxRaw, err := getRawTransaction(backrunTx)
	if err != nil {
		return fmt.Errorf("failed to get raw backrun transaction: %v", err)
	}

	// Create bundle
	bundle := BackrunBundle{
		Txs:               []string{origTxRaw, backrunTxRaw},
		RevertingTxHashes: []string{},
		MaxBlockNumber:    blockNumber + 2, // Valid for 2 blocks
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
		RefundAddress: config.RefundAddress.Hex(),
	}

	// Submit bundle
	bundleHash, err := submitBundle(bundle, config, false)
	if err != nil {
		return fmt.Errorf("failed to submit bundle: %v", err)
	}

	logger.Info("Successfully submitted bundle with hash: %s", bundleHash)
	return nil
}

// createBackrunTransactionV3 creates a V3 backrun transaction with improved error handling
func createBackrunTransactionV3(client *ethclient.Client, privateKey *ecdsa.PrivateKey,
	backrunInput *big.Int, path []PathElement, config Config) (*types.Transaction, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Get wallet address
	fromAddress := crypto.PubkeyToAddress(*privateKey.Public().(*ecdsa.PublicKey))

	// Get nonce
	nonce, err := client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get nonce: %v", err)
	}

	// Get gas price
	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get gas price: %v", err)
	}

	// Apply gas price multiplier
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(config.GasPriceMultiplier))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(100))

	// Respect max gas price
	if gasPrice.Cmp(config.MaxGasPrice) > 0 {
		gasPrice = new(big.Int).Set(config.MaxGasPrice)
	}

	// Load PancakeSwap router ABI
	routerABI, err := abi.JSON(strings.NewReader(PancakeRouterV3ABI))
	if err != nil {
		return nil, fmt.Errorf("failed to load router ABI: %v", err)
	}

	// Set deadline 5 minutes in the future
	deadline := big.NewInt(time.Now().Unix() + 300)

	// Determine if this is a multi-hop or single-hop swap
	var callData []byte
	var value *big.Int = big.NewInt(0) // Default to 0 ETH value

	// Log what we're doing
	if len(path) == 1 {
		logger.Info("Creating single-hop backrun tx: %s -> %s (fee: %d)",
			formatAddressOrToken(path[0].TokenIn, client),
			formatAddressOrToken(path[0].TokenOut, client),
			path[0].Fee)
	} else {
		pathStr := ""
		for i, p := range path {
			pathStr += fmt.Sprintf("%s->", formatAddressOrToken(p.TokenIn, client))
			if i == len(path)-1 {
				pathStr += formatAddressOrToken(p.TokenOut, client)
			}
		}
		logger.Info("Creating multi-hop backrun tx: %s", pathStr)
	}

	if len(path) == 1 {
		// Single-hop swap
		element := path[0]

		// If input token is WBNB, we send ETH directly
		if element.TokenIn == WBNB {
			// Estimate output amount with 3% slippage tolerance
			estOutput, err := quoteV3ExactInputSingle(ctx, client, element.TokenIn, element.TokenOut,
				element.Fee, backrunInput)
			if err != nil {
				return nil, fmt.Errorf("failed to estimate output: %v", err)
			}

			minOutput := new(big.Int).Mul(estOutput, big.NewInt(97))
			minOutput = new(big.Int).Div(minOutput, big.NewInt(100))

			logger.Info("ETH->Token backrun: Input %s BNB, Min output: %s tokens",
				formatEthValue(backrunInput), formatTokenAmount(minOutput))

			// Pack exactInputSingle call
			callData, err = routerABI.Pack("exactInputSingle",
				struct {
					TokenIn           common.Address
					TokenOut          common.Address
					Fee               uint32
					Recipient         common.Address
					Deadline          *big.Int
					AmountIn          *big.Int
					AmountOutMinimum  *big.Int
					SqrtPriceLimitX96 *big.Int
				}{
					TokenIn:           element.TokenIn,
					TokenOut:          element.TokenOut,
					Fee:               uint32(element.Fee),
					Recipient:         fromAddress,
					Deadline:          deadline,
					AmountIn:          backrunInput,
					AmountOutMinimum:  minOutput,
					SqrtPriceLimitX96: big.NewInt(0), // No price limit
				})

			if err != nil {
				return nil, fmt.Errorf("failed to pack exactInputSingle call: %v", err)
			}

			// Set ETH value
			value = backrunInput

		} else {
			// Normal ERC20 token input
			// Estimate output amount with 3% slippage tolerance
			estOutput, err := quoteV3ExactInputSingle(ctx, client, element.TokenIn, element.TokenOut,
				element.Fee, backrunInput)
			if err != nil {
				return nil, fmt.Errorf("failed to estimate output: %v", err)
			}

			minOutput := new(big.Int).Mul(estOutput, big.NewInt(97))
			minOutput = new(big.Int).Div(minOutput, big.NewInt(100))

			logger.Info("Token->Token/ETH backrun: Input %s tokens, Min output: %s",
				formatTokenAmount(backrunInput), formatTokenAmount(minOutput))

			// Pack exactInputSingle call
			callData, err = routerABI.Pack("exactInputSingle",
				struct {
					TokenIn           common.Address
					TokenOut          common.Address
					Fee               uint32
					Recipient         common.Address
					Deadline          *big.Int
					AmountIn          *big.Int
					AmountOutMinimum  *big.Int
					SqrtPriceLimitX96 *big.Int
				}{
					TokenIn:           element.TokenIn,
					TokenOut:          element.TokenOut,
					Fee:               element.Fee,
					Recipient:         fromAddress,
					Deadline:          deadline,
					AmountIn:          backrunInput,
					AmountOutMinimum:  minOutput,
					SqrtPriceLimitX96: big.NewInt(0), // No price limit
				})

			if err != nil {
				return nil, fmt.Errorf("failed to pack exactInputSingle call: %v", err)
			}
		}
	} else {
		// Multi-hop swap
		// Encode the path
		encodedPath := encodePancakeV3Path(path)

		// Check if first token is ETH/WBNB
		if path[0].TokenIn == WBNB {
			// Sending ETH for a multi-hop swap
			// Estimate output amount with 3% slippage tolerance
			estOutput, err := quoteV3ExactInputMultiHop(ctx, client, path, backrunInput)
			if err != nil {
				return nil, fmt.Errorf("failed to estimate output: %v", err)
			}

			minOutput := new(big.Int).Mul(estOutput, big.NewInt(97))
			minOutput = new(big.Int).Div(minOutput, big.NewInt(100))

			logger.Info("Multi-hop ETH->Token backrun: Input %s BNB, Min output: %s tokens",
				formatEthValue(backrunInput), formatTokenAmount(minOutput))

			// Pack exactInput call
			callData, err = routerABI.Pack("exactInput",
				struct {
					Path             []byte
					Recipient        common.Address
					Deadline         *big.Int
					AmountIn         *big.Int
					AmountOutMinimum *big.Int
				}{
					Path:             encodedPath,
					Recipient:        fromAddress,
					Deadline:         deadline,
					AmountIn:         backrunInput,
					AmountOutMinimum: minOutput,
				})

			if err != nil {
				return nil, fmt.Errorf("failed to pack exactInput call: %v", err)
			}

			// Set ETH value
			value = backrunInput

		} else {
			// Normal ERC20 token input for multi-hop
			// Estimate output amount with 3% slippage tolerance
			estOutput, err := quoteV3ExactInputMultiHop(ctx, client, path, backrunInput)
			if err != nil {
				return nil, fmt.Errorf("failed to estimate output: %v", err)
			}

			minOutput := new(big.Int).Mul(estOutput, big.NewInt(97))
			minOutput = new(big.Int).Div(minOutput, big.NewInt(100))

			logger.Info("Multi-hop Token->Token/ETH backrun: Input %s tokens, Min output: %s",
				formatTokenAmount(backrunInput), formatTokenAmount(minOutput))

			// Pack exactInput call
			callData, err = routerABI.Pack("exactInput",
				struct {
					Path             []byte
					Recipient        common.Address
					Deadline         *big.Int
					AmountIn         *big.Int
					AmountOutMinimum *big.Int
				}{
					Path:             encodedPath,
					Recipient:        fromAddress,
					Deadline:         deadline,
					AmountIn:         backrunInput,
					AmountOutMinimum: minOutput,
				})

			if err != nil {
				return nil, fmt.Errorf("failed to pack exactInput call: %v", err)
			}
		}
	}

	// Create transaction with higher gas limit for V3 swaps
	tx := types.NewTransaction(
		nonce,
		PancakeRouterV3,
		value,
		config.GasLimit,
		gasPrice,
		callData,
	)

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(big.NewInt(config.ChainID)), privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign transaction: %v", err)
	}

	return signedTx, nil
}

// checkAndApproveToken checks token allowance and creates approval tx if needed
func checkAndApproveToken(ctx context.Context, client *ethclient.Client, privateKey *ecdsa.PrivateKey,
	tokenAddress, spenderAddress common.Address, amount *big.Int, config Config) (*types.Transaction, error) {
	// Get wallet address
	fromAddress := crypto.PubkeyToAddress(*privateKey.Public().(*ecdsa.PublicKey))

	// Parse ERC20 ABI
	tokenABI, err := abi.JSON(strings.NewReader(ERC20ABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ERC20 ABI: %v", err)
	}

	// Check current token balance
	balanceData, err := callContractFunction(ctx, client, tokenAddress, tokenABI.Methods["balanceOf"], []interface{}{fromAddress})
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
	allowanceData, err := callContractFunction(ctx, client, tokenAddress, tokenABI.Methods["allowance"],
		[]interface{}{fromAddress, spenderAddress})
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
		logger.Info("Token allowance sufficient: %s (needed: %s)",
			formatTokenAmount(allowance), formatTokenAmount(amount))
		return nil, nil
	}

	logger.Info("Creating token approval transaction (current allowance: %s, needed: %s)",
		formatTokenAmount(allowance), formatTokenAmount(amount))

	// Get nonce
	nonce, err := client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get nonce: %v", err)
	}

	// Get gas price
	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get gas price: %v", err)
	}

	// Apply gas price multiplier
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(config.GasPriceMultiplier))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(100))

	// Respect max gas price
	if gasPrice.Cmp(config.MaxGasPrice) > 0 {
		gasPrice = new(big.Int).Set(config.MaxGasPrice)
	}

	// Create approval data
	// Use max uint256 value for unlimited approval
	maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

	approveData, err := tokenABI.Pack("approve", spenderAddress, maxUint256)
	if err != nil {
		return nil, fmt.Errorf("failed to pack approve call: %v", err)
	}

	// Create and sign transaction
	tx := types.NewTransaction(nonce, tokenAddress, big.NewInt(0), 100000, gasPrice, approveData)

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(big.NewInt(config.ChainID)), privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign approval transaction: %v", err)
	}

	return signedTx, nil
}

// simulateTransaction simulates a transaction execution
func simulateTransaction(ctx context.Context, client *ethclient.Client, from common.Address, tx *types.Transaction) error {
	msg := ethereum.CallMsg{
		From:     from,
		To:       tx.To(),
		Gas:      tx.Gas(),
		GasPrice: tx.GasPrice(),
		Value:    tx.Value(),
		Data:     tx.Data(),
	}

	_, err := client.CallContract(ctx, msg, nil)
	return err
}

// getRawTransaction converts a transaction to hex format
func getRawTransaction(tx *types.Transaction) (string, error) {
	data, err := tx.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("failed to marshal transaction: %v", err)
	}
	return hexutil.Encode(data), nil
}

// submitBundle submits a bundle to the relay
func submitBundle(bundle BackrunBundle, config Config, send bool) (string, error) {
	// Create JSON-RPC request
	request := JsonRpcRequest{
		JsonRpc: "2.0",
		Method:  "eth_sendBundle",
		Params:  []interface{}{bundle},
		ID:      1,
	}

	// Convert to JSON
	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON-RPC request: %v", err)
	}

	if !send {
		logger.Debug("Bundle: %s", string(jsonData))
		return "", nil
	}

	// Log request
	logger.Debug("Submitting bundle to %s", config.BlockRazorRpcURL)

	// Send request
	resp, err := http.Post(
		config.BlockRazorRpcURL,
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return "", fmt.Errorf("failed to send bundle: %v", err)
	}
	defer resp.Body.Close()

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	// Parse response
	var response JsonRpcResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return "", fmt.Errorf("failed to parse response: %v", err)
	}

	// Check for errors
	if response.Error != nil {
		return "", fmt.Errorf("relay error: %v", response.Error)
	}

	// Get bundle hash
	bundleHash, ok := response.Result.(string)
	if !ok {
		return "", fmt.Errorf("invalid response format")
	}

	return bundleHash, nil
}

// waitForTransaction waits for a transaction to be confirmed
func waitForTransaction(ctx context.Context, client *ethclient.Client, txHash common.Hash, confirmations uint64) (*types.Receipt, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			receipt, err := client.TransactionReceipt(ctx, txHash)
			if err != nil {
				if err == ethereum.NotFound {
					// Transaction not yet mined, continue waiting
					continue
				}
				return nil, err
			}

			// Get current block number
			currentBlock, err := client.BlockNumber(ctx)
			if err != nil {
				return nil, err
			}

			// Check confirmations
			if currentBlock >= receipt.BlockNumber.Uint64()+confirmations {
				return receipt, nil
			}

			// Not enough confirmations yet, continue waiting
			logger.Debug("Waiting for transaction %s to reach %d confirmations (current: %d)",
				txHash.Hex(), confirmations, currentBlock-receipt.BlockNumber.Uint64())
		}
	}
}

// callContractFunction is a helper to call contract functions
func callContractFunction(ctx context.Context, client *ethclient.Client, contractAddress common.Address, method abi.Method, args []interface{}) ([]byte, error) {
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
	return client.CallContract(ctx, msg, nil)
}

// Helper formatting functions
func formatTokenAmount(amount *big.Int) string {
	if amount == nil {
		return "0"
	}

	f := new(big.Float).SetInt(amount)
	f = new(big.Float).Quo(f, big.NewFloat(1e18)) // Assuming 18 decimals

	return f.Text('f', 6)
}

func formatEthValue(wei *big.Int) string {
	if wei == nil {
		return "0"
	}

	f := new(big.Float).SetInt(wei)
	f = new(big.Float).Quo(f, big.NewFloat(1e18))

	return f.Text('f', 6)
}

func formatAddressOrToken(address common.Address, client *ethclient.Client) string {
	// Check if this is WBNB
	if address == WBNB {
		return "BNB"
	}

	// Try to get token symbol
	tokenABI, err := abi.JSON(strings.NewReader(ERC20ABI))
	if err != nil {
		return address.Hex()
	}

	// Call symbol function
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := callContractFunction(ctx, client, address, tokenABI.Methods["symbol"], []interface{}{})
	if err != nil {
		return address.Hex()
	}

	var symbol string
	vals, err := tokenABI.Methods["symbol"].Outputs.Unpack(data)
	if err != nil {
		return address.Hex()
	}

	if len(vals) > 0 {
		if symbol, ok := vals[0].(string); ok {
			return symbol
		}
	}
	return fmt.Sprintf("%s (%s)", symbol, address.Hex()[:8]+"...")
}
