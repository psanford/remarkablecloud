package remarkablecloud

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

var (
	AuthHost           = "https://webapp-production-dot-remarkable-production.appspot.com"
	NewDeviceTokenPath = "/token/json/2/device/new"
	NewUserTokenPath   = "/token/json/2/user/new"
)

type Client struct {
	creds CredentialProvider
}

type CredentialProvider interface {
	Token() string // returns the current refresh token
}

func NewStaticTokenProvider(deviceToken, userToken string) CredentialProvider {
	return &staticCredential{
		userToken:   userToken,
		deviceToken: deviceToken,
	}
}

type staticCredential struct {
	deviceToken string
	userToken   string
}

func (s *staticCredential) Token() string {
	return s.userToken
}

func (s *staticCredential) Refresh() error {
	req, err := http.NewRequest("POST", AuthHost+NewUserTokenPath, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+s.deviceToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body err: %w", err)
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to refresh token, status=%d, body=%q", resp.StatusCode, body)
	}

	s.userToken = string(body)
	return nil
}
