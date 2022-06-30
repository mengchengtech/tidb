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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/mctech"
	"go.uber.org/zap"
)

const CRYPTO_PFEFIX = "{crypto}"

var cryptoPfrefixLength = len(CRYPTO_PFEFIX)

type AesCryptoClient struct {
	mock bool
	key  []byte
	iv   []byte
}

func newAesCryptoClientFromService() *AesCryptoClient {
	c := new(AesCryptoClient)
	option := mctech.GetMCTechOption()
	c.mock = option.Encryption_Mock

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
			} else {
				// 记录错误信息
				log.Error("Get mctech aes crypto KEY/IV FAILURE.", zap.Error(err))
				// 间隔10秒
				time.Sleep(10 * time.Second)
			}
		}
	}()
	return c
}

func NewAesCryptoClient(key string, iv string) *AesCryptoClient {
	c := new(AesCryptoClient)
	c.key, _ = base64.StdEncoding.DecodeString(key)
	c.iv, _ = base64.StdEncoding.DecodeString(iv)
	return c
}

func (c *AesCryptoClient) Encrypt(plainText string) (string, error) {
	if c.mock {
		// 用于调试场景
		return plainText, nil
	}

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
	cypher = CRYPTO_PFEFIX + base64.StdEncoding.EncodeToString(crypted)
	return cypher, nil
}

func (c *AesCryptoClient) Decrypt(content string) (string, error) {
	if c.mock {
		// 用于调试场景
		return content, nil
	}

	if !strings.HasPrefix(content, CRYPTO_PFEFIX) {
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

	//创建缓冲，存放解密后的数据
	orgData := make([]byte, len(cypher))
	//开始解密
	blockMode.CryptBlocks(orgData, cypher)
	//去掉编码
	orgData = pkcs7Unpadding(orgData)
	raw = string(orgData)
	return raw, nil
}

func loadCryptoParams(option *mctech.MCTechOption) (key []byte, iv []byte, err error) {
	// 从配置中获取
	apiPrefix := option.Encryption_ApiPrefix
	serviceUrl := apiPrefix + "db/aes"
	get, err := http.NewRequest("GET", serviceUrl, nil)
	if err != nil {
		return nil, nil, err
	}

	get.Header = map[string][]string{
		"x-access-id": {option.Encryption_AccessId},
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
	return data[:length-unpadding]
}

var client *AesCryptoClient
var cryptoInitOnce sync.Once

func GetClient() *AesCryptoClient {
	cryptoInitOnce.Do(func() {
		client = newAesCryptoClientFromService()
		log.Debug("init crypto client")
	})
	return client
}
