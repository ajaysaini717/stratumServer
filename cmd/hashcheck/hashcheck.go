package main

import (
	"fmt"
	"math/big"
	"asicPool/internal/pool/server"
)

// adjust import path to your repo

// expose a small helper in pool package if needed:
// func ScryptHashForTest(header []byte) common.Hash { return scryptHash(header) }

func main() {
	// // 1) Paste the fullHeader hex from your node log:
	// const fullHeaderHex = "00000020b8a01c650ac0ad983750684b752cc1f87ab878d2fd0fe79c268ff7c885e3341700000000000000000000000000000000000000000000000000000000000000008a213069ffff031fa22d0000" // <--- from DEBUG-POW-MATCH

	// fullHeader, err := hex.DecodeString(fullHeaderHex)
	// if err != nil {
	//     log.Fatalf("decode fullHeader: %v", err)
	// }
	// if len(fullHeader) != 80 {
	//     log.Fatalf("expected 80 bytes, got %d", len(fullHeader))
	// }

	// // 2) Hash with pool's scrypt implementation
	// h := pool.ScryptHash(fullHeader) // wraps your scryptHash()

	// // 3) Convert to bigint with same HashToBig as pool
	// hashInt := pool.HashToBig(&h)

	// fmt.Printf("POOL hash=%x\n", h[:])
	// fmt.Printf("POOL hashInt=%s\n", hashInt.Text(16))

	// // optional: compare with node-side hash printed in log
	// // (just eyeball or paste node hash here and print both)

	compact := BigToCompact(server.Diff1TargetPool)
	fmt.Println("compact:", fmt.Sprintf("0x%08x", compact))

}

func BigToCompact(n *big.Int) uint32 {
	// No need to do any work if it's zero.
	if n.Sign() == 0 {
		return 0
	}

	// Since the base for the exponent is 256, the exponent can be treated
	// as the number of bytes.  So, shift the number right or left
	// accordingly.  This is equivalent to:
	// mantissa = mantissa / 256^(exponent-3)
	var mantissa uint32
	exponent := uint(len(n.Bytes()))
	if exponent <= 3 {
		mantissa = uint32(n.Bits()[0])
		mantissa <<= 8 * (3 - exponent)
	} else {
		// Use a copy to avoid modifying the caller's original number.
		tn := new(big.Int).Set(n)
		mantissa = uint32(tn.Rsh(tn, 8*(exponent-3)).Bits()[0])
	}

	// When the mantissa already has the sign bit set, the number is too
	// large to fit into the available 23-bits, so divide the number by 256
	// and increment the exponent accordingly.
	if mantissa&0x00800000 != 0 {
		mantissa >>= 8
		exponent++
	}

	// Pack the exponent, sign bit, and mantissa into an unsigned 32-bit
	// int and return it.
	compact := uint32(exponent<<24) | mantissa
	if n.Sign() < 0 {
		compact |= 0x00800000
	}
	return compact
}
