package main

import (
	"context"
	"log"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/joho/godotenv"
)

var tWBNB = "0xae13d989dac2f0debff460ac112a837c89baa7cd"

var tBUSD = "0xaB1a4d4f1D656d2450692D237fdD6C7f9146e814"

var USDT = "0x55d398326f99059ff775485246999027b3197955"

func TestNewMEVBot(t *testing.T) {

	err := godotenv.Load("../../testnet.env")
	if err != nil {
		log.Println("Warning: .env file not found")
	}

	// Initialize configuration
	config := loadConfig()

	bot, err := NewMEVBot(config)
	if err != nil {
		t.Fatalf("Failed to create MEVBot: %v", err)
	}
	backrunInput := big.NewInt(1000)
	minOutput := big.NewInt(500) // 0.01 BNB

	var reversedPath []common.Address
	reversedPath = append(reversedPath, common.HexToAddress(tWBNB))
	reversedPath = append(reversedPath, common.HexToAddress(tBUSD))

	deadline := big.NewInt(1778850693) // 2026-5

	backrunMethod := "executeBackrunTokenToToken"
	backrunParams := []interface{}{
		common.HexToAddress("0xD99D1c33F9fC3444f8101754aBC46c52416550D1"), // testnet pancake router v2
		backrunInput,
		minOutput,
		reversedPath,
		deadline,
	}
	backrunContractABI, err := abi.JSON(strings.NewReader(BackrunABI))

	// Pack the call data for our backrun contract
	callData, err := backrunContractABI.Pack(backrunMethod, backrunParams...)
	if err != nil {
		t.Fatalf("Failed to pack call data: %v", err)
	}

	gasPrice, err := bot.client.SuggestGasPrice(context.Background())
	if err != nil {
		t.Fatalf("Failed to suggest gas price: %v", err)
	}
	gasTip, err := bot.client.SuggestGasTipCap(context.Background())
	if err != nil {
		t.Fatalf("Failed to suggest gas tip: %v", err)
	}

	// Create dynamic fee transaction instead of legacy
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   big.NewInt(bot.config.ChainID),
		Nonce:     uint64(bot.nonce.Load()),
		To:        &bot.config.BackrunContractAddress, // Address of our backrun contract
		Value:     big.NewInt(0),
		Gas:       uint64(500000),
		GasTipCap: gasTip,
		GasFeeCap: gasPrice,
		Data:      callData,
	})
	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewLondonSigner(big.NewInt(bot.config.ChainID)), bot.privateKey)
	if err != nil {
		t.Fatalf("Failed to sign transaction: %v", err)
	}
	t.Log("Signed transaction: ", signedTx.Hash().Hex())

	err = bot.client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		t.Fatalf("Failed to send transaction: %v", err)
	}
	t.Log("Transaction sent: ", signedTx.Hash().Hex())
}

func TestNewMEVBotMainnet(t *testing.T) {

	err := godotenv.Load("../../mainnet.env")
	if err != nil {
		log.Println("Warning: .env file not found")
	}

	// Initialize configuration
	config := loadConfig()

	bot, err := NewMEVBot(config)
	if err != nil {
		t.Fatalf("Failed to create MEVBot: %v", err)
	}
	backrunInput := big.NewInt(1000000000000)
	minOutput := big.NewInt(500)

	var reversedPath []common.Address
	reversedPath = append(reversedPath, common.HexToAddress(USDT))
	reversedPath = append(reversedPath, WBNB)

	deadline := big.NewInt(1778850693) // 2026-5

	backrunMethod := "executeBackrunTokenToToken"

	backrunParams := []interface{}{
		PancakeRouterV2,
		backrunInput,
		minOutput,
		reversedPath,
		deadline,
		common.HexToAddress("0x74Ce839c6aDff544139f27C1257D34944B794605"),
		common.HexToAddress("0x69dD7D445f14cB22BE42ba268479723770d4C660"),
		big.NewInt(int64(112345)),
		big.NewInt(1),
	}
	backrunContractABI, err := abi.JSON(strings.NewReader(BackrunABI))

	// Pack the call data for our backrun contract
	callData, err := backrunContractABI.Pack(backrunMethod, backrunParams...)
	if err != nil {
		t.Fatalf("Failed to pack call data: %v", err)
	}

	gasPrice, err := bot.client.SuggestGasPrice(context.Background())
	if err != nil {
		t.Fatalf("Failed to suggest gas price: %v", err)
	}
	gasTip, err := bot.client.SuggestGasTipCap(context.Background())
	if err != nil {
		t.Fatalf("Failed to suggest gas tip: %v", err)
	}

	// Create dynamic fee transaction instead of legacy
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   big.NewInt(bot.config.ChainID),
		Nonce:     uint64(bot.nonce.Load()),
		To:        &bot.config.BackrunContractAddress, // Address of our backrun contract
		Value:     big.NewInt(2),
		Gas:       uint64(500000),
		GasTipCap: gasTip,
		GasFeeCap: gasPrice,
		Data:      callData,
	})
	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewLondonSigner(big.NewInt(bot.config.ChainID)), bot.privateKey)
	if err != nil {
		t.Fatalf("Failed to sign transaction: %v", err)
	}
	t.Log("Signed transaction: ", signedTx.Hash().Hex())

	err = bot.client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		t.Fatalf("Failed to send transaction: %v", err)
	}
	t.Log("Transaction sent: ", signedTx.Hash().Hex())
}
