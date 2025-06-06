blockdag stratum protocol
====


## mining.subscribe

```
params: ["agent", null]
result: [null, "magicnumber", "nonce size"]

magicnumber is last two bytes of the block header in little endian hex.

request:

{
  "id": 1,
  "method": "mining.subscribe",
  "params": ["kdaminer-v1.0.0", null]
}

response:

{
  "id": 1,
  "result": [null, "1122", 8],
  "error": null
}
```


## mining.authorize

```
params: ["username", "password"]
result: true

request:

{
  "id": 2,
  "method": "mining.authorize",
  "params": ["testuser", "testpass"]
}

response:

{
  "id": 2,
  "result": true,
  "error": null
}
```


## mining.set_target

```
params: ["32 bytes target in big endian hex"]

{
  "id": null,
  "method": "mining.set_target",
  "params": ["0001000000000000000000000000000000000000000000000000000000000000"]
}
```


## mining.notify

```
params: ["jobId", "header", cleanJob]

{
  "id": null,
  "method": "mining.notify",
  "params": [
    "1234",
    "150 bytes header in hex",
    true
  ]
}

There should be rganized according to blockdag's definition. Total size 150 bytes, all data in stratum should be as little endian hex.
for example 
00000000000000004fd7c33d212c06007e3a5ee18306b2ff33d82f34bdbb429c5330a902e7db0286a43fd33999bd5dec0300040000009d3f482df44b11a178c6b51209d262d32b77a0f48d89f7c21bd9e4cef1b90e7c06000000b597e466f2d46b790ae4352ab2270e388fc96447c59828a839a222e0c56532dc07000000483d2991e9ead2ff19f7c0eaae8781cd600cbeee58361aaa

Follow is Kadena's sample
https://github.com/kadena-io/chainweb-node/wiki/Block-Header-Binary-Encoding
Size    Bytes    Value
8       0-7      flags
8       8-15     time
32      16-47    parent
110     48-157   adjacents
32      158-189  target
32      190-221  payload
4       222-225  chain
32      226-257  weight
8       258-265  height
4       266-269  version
8       270-277  epoch start
8       278-285  nonce
```


## mining.submit
**old version**

```
params: ["username.worker", "jobId", "nonce"]
result: true / false

request:

{
  "id": 102,
  "method": "mining.submit",
  "params": [
    "testuser.worker1",
    "1234",
    "feb3020000000000"
  ]
}
```

```
response:

accepted share response:

{
  "id": 102,
  "result": true,
  "error": null
}


rejected share response:

{
  "id": 102,
  "result": false,
  "error": [21, "low difficulty share", null]
}

```

for not standard blake2s hash, g_extendFlag value 223(see the go source code of vendor\golang.org\x\crypto\blake2sext\blake2sext.go)
// sample1 160 inputdata
00000000000000004fd7c33d212c06007e3a5ee18306b2ff33d82f34bdbb429c5330a902e7db0286a43fd33999bd5dec0300040000009d3f482df44b11a178c6b51209d262d32b77a0f48d89f7c21bd9e4cef1b90e7c06000000b597e466f2d46b790ae4352ab2270e388fc96447c59828a839a222e0c56532dc07000000483d2991e9ead2ff19f7c0eaae8781cd600cbeee58361aaafeb30200000000001122
// hash result
746ede34f8ee79e9686872d96cd0e9ef0152c480fb6447c0b06a8d96b8ec0000

// sample2 160 inputdata
00000000000000004fd7c33d212c06007e3a5ee18306b2ff33d82f34bdbb429c5330a902e7db0286a43fd33999bd5dec0300040000009d3f482df44b11a178c6b51209d262d32b77a0f48d89f7c21bd9e4cef1b90e7c06000000b597e466f2d46b790ae4352ab2270e388fc96447c59828a839a222e0c56532dc07000000483d2991e9ead2ff19f7c0eaae8781cd600cbeee58361aaa241aa27e010000001122
// hash result
af7cd035bf86c03c9474f650bdd295d6da378e763db222a4a53d7d1c00000000


