package main

import (
	"io/ioutil"

	"github.com/psanford/remarkablecloud"
	"gopkg.in/yaml.v2"
)

func newClient() (*remarkablecloud.Client, error) {
	content, err := ioutil.ReadFile("/home/psanford/.config/rmapi/rmapi.conf")
	if err != nil {
		return nil, err
	}

	var tokens AuthTokens
	err = yaml.Unmarshal(content, &tokens)
	if err != nil {
		return nil, err
	}

	creds := remarkablecloud.NewStaticTokenProvider(tokens.DeviceToken, tokens.UserToken)

	err = creds.(refreshable).Refresh()
	if err != nil {
		return nil, err
	}

	client := remarkablecloud.New(creds)

	return client, nil
}
