package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

func submitBlockHeader(fullHeaderHex string, extraNonce2 uint64) (interface{}, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "submitBlockHeader",
		"params":  []interface{}{fullHeaderHex, extraNonce2},
	}
	buf, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", rpcURL, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(rpcUser, rpcPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rpcResp struct {
		Result interface{} `json:"result"`
		Error  interface{} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, err
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %v", rpcResp.Error)
	}

	return rpcResp.Result, nil
}
