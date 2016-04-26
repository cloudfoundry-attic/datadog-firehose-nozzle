package uaatokenfetcher

import (
	"github.com/cloudfoundry-incubator/uaago"
	"github.com/cloudfoundry/gosteno"
)

type UAATokenFetcher struct {
	uaaUrl                string
	username              string
	password              string
	insecureSSLSkipVerify bool
	log                   *gosteno.Logger
}

func New(uaaUrl string, username string, password string, sslSkipVerify bool, logger *gosteno.Logger) *UAATokenFetcher {
	return &UAATokenFetcher{
		uaaUrl:                uaaUrl,
		username:              username,
		password:              password,
		insecureSSLSkipVerify: sslSkipVerify,
		log: logger,
	}
}

func (uaa *UAATokenFetcher) FetchAuthToken() string {
	uaaClient, err := uaago.NewClient(uaa.uaaUrl)
	if err != nil {
		uaa.log.Fatalf("Error creating uaa client: %s", err.Error())
	}

	var authToken string
	authToken, err = uaaClient.GetAuthToken(uaa.username, uaa.password, uaa.insecureSSLSkipVerify)
	if err != nil {
		uaa.log.Fatalf("Error getting oauth token: %s. Please check your username and password.", err.Error())
	}
	return authToken
}
