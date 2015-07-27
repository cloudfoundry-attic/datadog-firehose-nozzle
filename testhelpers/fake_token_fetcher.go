package testhelpers

type FakeTokenFetcher struct {
	NumCalls int
}

func (tokenFetcher *FakeTokenFetcher) FetchAuthToken() string {
	tokenFetcher.NumCalls++
	return "auth token"
}
