package sessionctx

import "strings"

func ResolveSession(sctx Context) (db string, user string, client string) {
	sessVars := sctx.GetSessionVars()
	db = strings.ToLower(sessVars.CurrentDB)
	if sessVars.User != nil {
		user = sessVars.User.Username
		client = sessVars.User.Hostname
	}

	return db, user, client
}
