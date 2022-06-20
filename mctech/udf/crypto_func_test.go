package udf

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var mockClient *AesCryptoClient

func init() {
	key := "W1gfHNQTARa7Uxt7wua8Aw=="
	iv := "a9Z5R6YCjYx1QmoG5WF9BQ=="
	mockClient = NewAesCryptoClient(key, iv)
}

func TestMCTechEncryptSuccess(t *testing.T) {
	cipher, err := mockClient.Encrypt("13511868785")
	require.NoError(t, err)
	fmt.Println(cipher)
	require.Equal(t, "{crypto}HMvlbGus4V3geqwFULvOUw==", cipher)
}

func TestMCTechDecryptSuccess(t *testing.T) {
	plain, err := mockClient.Decrypt("{crypto}HMvlbGus4V3geqwFULvOUw==")
	require.NoError(t, err)
	require.Equal(t, "13511868785", plain)
}

func TestMCTechDecryptFailure1(t *testing.T) {
	plain, err := mockClient.Decrypt("YEsSIc6gxBu7NJt8De3fxg==")
	require.NoError(t, err)
	require.Equal(t, "YEsSIc6gxBu7NJt8De3fxg==", plain)
}

func TestMCTechDecryptFailure2(t *testing.T) {
	_, err := mockClient.Decrypt("{crypto}YEsSIc6gxBu7NJt8De3fxg=")
	require.ErrorContains(t, err, "illegal base64 data at input byte")
}
