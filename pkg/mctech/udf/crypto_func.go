package udf

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const cryptoPrefix = "{crypto}"

var cryptoPfrefixLength = len(cryptoPrefix)

// CryptoClient crypto client
type CryptoClient interface {
	// Encrypt text
	Encrypt(plainText string) (string, error)
	// Decrypt text
	Decrypt(content string) (string, error)
}

type mockCryptoClient struct {
}

// Encrypt text
func (p *mockCryptoClient) Encrypt(plainText string) (string, error) {
	return plainText, nil
}

// Decrypt text
func (p *mockCryptoClient) Decrypt(content string) (string, error) {
	return content, nil
}

type aesCryptoClient struct {
	option *config.MCTech
	key    []byte
	iv     []byte
}

func newAesCryptoClientFromService() CryptoClient {
	option := config.GetMCTechConfig()
	if option.Encryption.Mock {
		return &mockCryptoClient{}
	}

	c := new(aesCryptoClient)
	c.option = option

	key, iv, err := loadCryptoParams(option)
	if err == nil {
		log.Info("GET mctech aes crypto KEY/IV SUCCESS. ")
		c.key = key
		c.iv = iv
		return c
	}

	// 记录错误信息
	log.Error("Get mctech aes crypto KEY/IV FAILURE.", zap.Error(err))

	// 转成后台定时加载
	go func() {
		for {
			key, iv, err = loadCryptoParams(option)
			log.Info("GET mctech aes crypto KEY/IV SUCCESS. ")
			// 加载成功退出后台执行
			if err == nil {
				c.key = key
				c.iv = iv
				break
			}
			// 记录错误信息
			log.Error("Get mctech aes crypto KEY/IV FAILURE.", zap.Error(err))
			// 间隔10秒
			time.Sleep(10 * time.Second)
		}
	}()
	return c
}

// Encrypt encrypt plain text
func (c *aesCryptoClient) Encrypt(plainText string) (string, error) {
	var cypher string
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return cypher, err
	}

	//设置加密方式
	blockMode := cipher.NewCBCEncrypter(block, c.iv)

	orig := []byte(plainText)
	//补码
	origData := pkcs7Padding(orig, block.BlockSize())
	//加密处理
	crypted := make([]byte, len(origData)) //存放加密后的密文
	blockMode.CryptBlocks(crypted, origData)
	cypher = cryptoPrefix + base64.StdEncoding.EncodeToString(crypted)
	return cypher, nil
}

// Decrypt decrypt cipher text
func (c *aesCryptoClient) Decrypt(content string) (s string, err error) {
	if !strings.HasPrefix(content, cryptoPrefix) {
		return content, nil
	}

	var raw string
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return raw, err
	}
	//设置解密方式
	blockMode := cipher.NewCBCDecrypter(block, c.iv)
	cypher, err := base64.StdEncoding.DecodeString(content[cryptoPfrefixLength:])
	if err != nil {
		return raw, err
	}

	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				logutil.BgLogger().Error("mctech decrypt failure", zap.String("input", content), zap.Error(err))
			} else {
				logutil.BgLogger().Error(fmt.Sprintf("mctech decrypt failure, %v", r), zap.String("input", content))
			}
			err = fmt.Errorf("mctech decrypt failure. '%s'", content)
		}
	}()
	//创建缓冲，存放解密后的数据
	orgData := make([]byte, len(cypher))
	//开始解密
	blockMode.CryptBlocks(orgData, cypher)
	//去掉编码
	orgData = pkcs7Unpadding(orgData)
	raw = string(orgData)
	return raw, nil
}

func loadCryptoParams(option *config.MCTech) (key []byte, iv []byte, err error) {
	// 从配置中获取
	apiPrefix := option.Encryption.APIPrefix
	serviceURL := apiPrefix + "db/aes"
	get, err := http.NewRequest("GET", serviceURL, nil)
	if err != nil {
		return nil, nil, err
	}

	get.Header = map[string][]string{
		"x-access-id": {option.Encryption.AccessID},
	}

	body, err := mctech.DoRequest(get)
	if err != nil {
		return nil, nil, err
	}

	var tokens = map[string]string{}
	err = json.Unmarshal(body, &tokens)
	if err != nil {
		return nil, nil, err
	}
	key, err = base64.StdEncoding.DecodeString(tokens["key"])
	if err != nil {
		return nil, nil, err
	}
	iv, err = base64.StdEncoding.DecodeString(tokens["iv"])
	if err != nil {
		return nil, nil, err
	}

	if len(key) == 0 || len(iv) == 0 {
		return nil, nil, fmt.Errorf("key或者iv不能为空")
	}
	return key, iv, nil
}

func pkcs7Padding(data []byte, blockSize int) []byte {
	padLen := blockSize - len(data)%blockSize
	padding := bytes.Repeat([]byte{byte(padLen)}, padLen)
	return append(data, padding...)
}

func pkcs7Unpadding(data []byte) []byte {
	length := len(data)
	unpadding := int(data[length-1])

	failpoint.Inject("pkcs7Unpadding", func() {
		unpadding = length + 137
	})
	return data[:length-unpadding]
}

var client CryptoClient
var cryptoInitOnce sync.Once

// GetClient get crypto client
func GetClient() CryptoClient {
	if client != nil {
		return client
	}

	cryptoInitOnce.Do(func() {
		client = newAesCryptoClientFromService()
		log.Debug("init crypto client")
	})
	return client
}
