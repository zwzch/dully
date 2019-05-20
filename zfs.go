package main

import (

	"flag"
	"dully/net"
	"dully/conf"
	"fmt"
)

func Start()  {
	conf.ParseFlag()
	flag.Parse()
	fmt.Println(conf.CF)
	fmt.Println(conf.Cmd)
	net.Start()
}

func main()  {
	Start()

}