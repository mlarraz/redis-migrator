package main

import (
	"fmt"
	"github.com/huantt/redis-migrator/cmd"
)

var (
	GitCommit string
	Version   string
)

func main() {
	version := fmt.Sprintf("%s: %s", Version, GitCommit)
	cmd.Execute(version)
}
