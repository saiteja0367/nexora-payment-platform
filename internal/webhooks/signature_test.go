package webhooks

import (
	"testing"
	"time"
)

func TestSignVerify(t *testing.T) {
	secret := []byte("at-least-sixteen-byte-secret")
	now := time.Unix(1700000000, 0)
	msg := []byte(`{"payment_id":"p1"}`)
	sig, err := Sign(secret, now.Unix(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if !Verify(secret, now.Unix(), msg, sig, now, 5*time.Minute) {
		t.Fatal("valid signature rejected")
	}
	if Verify(secret, now.Unix(), []byte("tampered"), sig, now, 5*time.Minute) {
		t.Fatal("tampered payload accepted")
	}
	if Verify(secret, now.Unix(), msg, sig, now.Add(10*time.Minute), 5*time.Minute) {
		t.Fatal("stale signature accepted")
	}
	if Verify([]byte("other-secret-long-enough"), now.Unix(), msg, sig, now, 5*time.Minute) {
		t.Fatal("wrong secret accepted")
	}
}
