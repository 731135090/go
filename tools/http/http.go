package http

import (
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const DEFAULT_TIME_OUT = 60 * time.Second
const DEFAULT_CONTENT_TYPE = "application/x-www-form-urlencoded; charset=utf-8"

type GetClient struct {
	Url     string
	Host    string
	TimeOut time.Duration
}

type PostClient struct {
	Url     string
	Host    string
	TimeOut time.Duration
	Data    map[string]string
}

func (client *GetClient) Get() (string, error) {
	Timeout := client.TimeOut
	if Timeout == 0 {
		Timeout = DEFAULT_TIME_OUT
	}
	httpClient := &http.Client{Timeout: Timeout}
	request, err := http.NewRequest("GET", client.Url, nil)
	if err != nil {
		return "", err
	}
	if client.Host != "" {
		request.Host = client.Host
	}
	request.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36")
	response, err := httpClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode == 200 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return "", err
		}
		return string(body), nil
	}
	return "", errors.New("http code error : " + strconv.Itoa(response.StatusCode))
}

func (client *PostClient) Post() (string, error) {
	var bodyStr string
	Timeout := client.TimeOut
	if Timeout == 0 {
		Timeout = DEFAULT_TIME_OUT
	}
	httpClient := &http.Client{Timeout: Timeout}
	request, err := http.NewRequest("POST", client.Url, postData(client.Data))
	if err != nil {
		return "", err
	}
	if client.Host != "" {
		request.Host = client.Host
	}
	request.Header.Set("Content-Type", DEFAULT_CONTENT_TYPE)

	response, err := httpClient.Do(request)
	if err != nil {
		return "", err
	}

	defer response.Body.Close()
	if response.StatusCode == 200 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return "", err
		}
		bodyStr = string(body)
		return bodyStr, nil
	}
	return bodyStr, errors.New(strconv.Itoa(response.StatusCode))
}

func postData(data map[string]string) io.Reader {
	d := url.Values{}
	for k, v := range data {
		d.Set(k, v)
	}
	return strings.NewReader(d.Encode())
}
