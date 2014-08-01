// Copyright (C) 2014 Daniel Harrison

package main

import "github.com/paperstreet/gohfile/hfile"
import "flag"
import "fmt"

var configs hfile.ServerConfigs

func init() {
	flag.Var(&configs, "config", "hfile server configs")
}

func main() {
	flag.Parse()
	server, err := hfile.NewServer(configs)
	if err != nil {
		fmt.Println(err)
		return
	}
	server.Start()
}
