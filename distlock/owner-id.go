package distlock

import (
	"encoding/base64"
	"crypto/rand"
)

func generateRandomOwnerId() string {
	// UUID is 128 bits (16 bytes), 122 of which are entropy, so going with at least that.
	// Source: http://www.h2database.com/html/advanced.html#uuid
	entropy := make([]byte, 16)
	rand.Read(entropy)
	return base64.URLEncoding.EncodeToString(entropy)
}
