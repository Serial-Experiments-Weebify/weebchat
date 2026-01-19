package main

import (
	"log"
	"net"

	"github.com/Serial-Experiments-Weebify/weebchat/core"
	"github.com/alecthomas/kong"
)

var CLI struct {
	Data struct {
		Control string `help:"Control server address" default:"localhost:6767" short:"c"`

		SecretKey string `required:"" help:"Secret key for node authentication." short:"s"`

		Address string `optional:"" help:"Address to listen on." short:"a"`
	} `cmd:"" help:"Start the data server."`

	Control struct {
		Address   string `optional:"" help:"Address to listen on." default:"localhost:6767" short:"a"`
		SecretKey string `required:"" help:"Secret key for node authentication." short:"s"`
	} `cmd:"" help:"Start the control server."`
}

func createListener(addr string) (net.Listener, error) {
	if addr != "" {
		return net.Listen("tcp", addr)
	} else {
		return net.Listen("tcp", "localhost:0")
	}
}

func main() {
	ctx := kong.Parse(&CLI)

	switch ctx.Command() {

	case "data":
		lis, err := createListener(CLI.Data.Address)
		if err != nil {
			panic(err)
		}

		log.Printf("Data server starting on %s (control: %s)", lis.Addr().String(), CLI.Data.Control)

		srv, err := core.StartDataServer(lis.Addr().String(), CLI.Data.Control, CLI.Data.SecretKey)

		if err != nil {
			panic(err)
		}

		if err := srv.Serve(lis); err != nil {
			panic(err)
		}

	case "control":
		lis, err := createListener(CLI.Control.Address)

		if err != nil {
			panic(err)
		}

		log.Printf("Control plane server starting on %s", lis.Addr().String())

		srv := core.StartControlServer(lis.Addr().String(), CLI.Control.SecretKey)

		if err := srv.Serve(lis); err != nil {
			panic(err)
		}
	}
}
