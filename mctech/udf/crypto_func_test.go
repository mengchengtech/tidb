package udf

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/stretchr/testify/require"
)

type encryptionTestCases struct {
	encrypt bool
	input   string
	expect  string
	failure string
}

func TestMCTechCrypto(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Encryption.Mock": false}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")

	cases := []*encryptionTestCases{
		{true, "13511868785", "{crypto}HMvlbGus4V3geqwFULvOUw==", ""},
		{false, "{crypto}HMvlbGus4V3geqwFULvOUw==", "13511868785", ""},
		{false, "{crypto}YEsSIc6gxBu7NJt8De3fxg=", "", "illegal base64 data at input byte"},
	}

	doRunCryptoTest(t, cases)
}

func TestMCTechCryptoUnpaddingFail(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Encryption.Mock": false}),
	)
	failpoint.Enable("github.com/pingcap/tidb/mctech/udf/pkcs7Unpadding",
		mock.M(t, "true"),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/udf/pkcs7Unpadding")
	}()

	cases := []*encryptionTestCases{
		{false, "{crypto}XGhHgHevnICYzqxUPS26lw==", "", "mctech decrypt failure. '{crypto}XGhHgHevnICYzqxUPS26lw=='"},
	}

	doRunCryptoTest(t, cases)
}

func doRunCryptoTest(t *testing.T, cases []*encryptionTestCases) {
	failpoint.Enable("github.com/pingcap/tidb/mctech/MockMctechHttp",
		mock.M(t, map[string]any{
			"Crypto.AES": map[string]string{"key": "W1gfHNQTARa7Uxt7wua8Aw==", "iv": "a9Z5R6YCjYx1QmoG5WF9BQ=="},
		}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/mctech/MockMctechHttp")

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
