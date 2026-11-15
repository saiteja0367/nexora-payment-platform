// Package webhooks provides deterministic HMAC signatures for outbound webhook payloads.
package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Sign returns a versioned signature over timestamp + '.' + the exact payload bytes.
func Sign(secret []byte, timestamp int64, payload []byte) (string, error) {
	if len(secret) < 16 {
		return "", errors.New("webhook secret must be at least 16 bytes")
	}
	m := hmac.New(sha256.New, secret)
	fmt.Fprintf(m, "%d.", timestamp)
	_, _ = m.Write(payload)
	return "v1=" + hex.EncodeToString(m.Sum(nil)), nil
}

// Verify rejects stale or future-dated requests and compares MACs in constant time.
func Verify(secret []byte, timestamp int64, payload []byte, signature string, now time.Time, tolerance time.Duration) bool {
	if tolerance <= 0 || timestamp < 0 {
		return false
	}
	t := time.Unix(timestamp, 0)
	if t.Before(now.Add(-tolerance)) || t.After(now.Add(tolerance)) {
		return false
	}
	expected, err := Sign(secret, timestamp, payload)
	if err != nil {
		return false
	}
	a, e1 := hex.DecodeString(strings.TrimPrefix(signature, "v1="))
	b, e2 := hex.DecodeString(strings.TrimPrefix(expected, "v1="))
	return strings.HasPrefix(signature, "v1=") && e1 == nil && e2 == nil && hmac.Equal(a, b)
}

func TimestampHeader(now time.Time) string { return strconv.FormatInt(now.Unix(), 10) }
