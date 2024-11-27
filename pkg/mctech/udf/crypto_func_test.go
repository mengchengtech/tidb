package udf

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/stretchr/testify/require"
)

type encryptionTestCases struct {
	encrypt bool
	input   string
	expect  string
	failure string
}

func TestMCTechCrypto(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Encryption.Mock": false}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	cases := []*encryptionTestCases{
		{true, "13511868785", "{crypto}HMvlbGus4V3geqwFULvOUw==", ""},
		{false, "{crypto}HMvlbGus4V3geqwFULvOUw==", "13511868785", ""},
		{false, "{crypto}YEsSIc6gxBu7NJt8De3fxg=", "", "illegal base64 data at input byte"},
	}

	doRunCryptoTest(t, cases)
}

func doRunCryptoTest(t *testing.T, cases []*encryptionTestCases) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp",
		mock.M(t, map[string]any{
			"Crypto.AES": map[string]string{"key": "W1gfHNQTARa7Uxt7wua8Aw==", "iv": "a9Z5R6YCjYx1QmoG5WF9BQ=="},
		}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp")

	client := newAesCryptoClientFromService()
	for _, c := range cases {
		var (
			err  error
			text string
		)
		if c.encrypt {
			text, err = client.Encrypt(c.input)
		} else {
			text, err = client.Decrypt(c.input)
		}

		if err != nil {
			if c.failure != "" {
				require.ErrorContains(t, err, c.failure)
			} else {
				require.NoError(t, err)
			}
		} else {
			require.Equal(t, c.expect, text)
		}
	}
}
