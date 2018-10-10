package cli

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/buraksezer/olricdb/client"
	"github.com/chzyer/readline"
	"golang.org/x/net/http2"
)

type CLI struct {
	uri    string
	client *client.Client
	output io.Writer
}

func New(uri string, insecureSkipVerify bool, dialerTimeouts, timeouts string) (*CLI, error) {
	dialerTimeout, err := time.ParseDuration(dialerTimeouts)
	if err != nil {
		return nil, err
	}
	timeout, err := time.ParseDuration(timeouts)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	var hclient *http.Client
	if parsed.Scheme == "https" {
		tc := &tls.Config{InsecureSkipVerify: insecureSkipVerify}
		dialTLS := func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			d := &net.Dialer{Timeout: dialerTimeout}
			return tls.DialWithDialer(d, network, addr, cfg)
		}
		hclient = &http.Client{
			Transport: &http2.Transport{
				DialTLS:         dialTLS,
				TLSClientConfig: tc,
			},
			Timeout: timeout,
		}
	} else {
		hclient = &http.Client{}
	}

	c, err := client.New([]string{uri}, hclient, nil)
	if err != nil {
		return nil, err
	}
	return &CLI{
		uri:    uri,
		client: c,
	}, nil
}

var completer = readline.NewPrefixCompleter(
	readline.PcItem("help"),
	readline.PcItem("use"),
	readline.PcItem("put"),
	readline.PcItem("putex"),
	readline.PcItem("get"),
	readline.PcItem("delete"),
	readline.PcItem("destroy"),
)

func (c *CLI) print(msg string) {
	io.WriteString(c.output, msg)
}

func (c *CLI) Start() error {
	var historyFile string
	home := os.Getenv("HOME")
	if home != "" {
		historyFile = path.Join(home, ".olricdbcli_history")
	} else {
		c.print("[WARN] $HOME is empty.\n")
	}
	prompt := fmt.Sprintf("[%s] \033[31m»\033[0m ", c.uri)
	l, err := readline.NewEx(&readline.Config{
		Prompt:          prompt,
		HistoryFile:     historyFile,
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()
	c.output = l.Stderr()

	var dmap string
	for {
		line, err := l.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "use "):
			tmp := strings.Split(line, "use")[1]
			dmap = strings.Trim(tmp, " ")
			continue
		case strings.HasPrefix(line, "help"):
			c.print("commands:\n")
			c.print(completer.Tree("    "))
			continue
		}

		if len(dmap) == 0 {
			c.print("Call 'use' command before accessing database.\n")
			continue
		}

		switch {
		case strings.HasPrefix(line, "put "):
			tmp := strings.TrimLeft(line, "put ")
			parsed := strings.SplitN(tmp, " ", 2)
			key, value := parsed[0], parsed[1]
			if err := c.client.Put(dmap, key, value); err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Put on %s with key %s: %v\n", dmap, key, err))
			}
			c.print("OK\n")
		case strings.HasPrefix(line, "get "):
			key := strings.TrimLeft(line, "get ")
			value, err := c.client.Get(dmap, key)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Get on %s with key %s: %v\n", dmap, key, err))
			}
			c.print(fmt.Sprintf("%v\n", value))
		case strings.HasPrefix(line, "delete "):
			key := strings.TrimLeft(line, "delete ")
			err := c.client.Delete(dmap, key)
			if err != nil {
				c.print(fmt.Sprintf("[ERROR] Failed to call Get on %s with key %s: %v\n", dmap, key, err))
			}
			c.print("OK\n")
		default:
			c.print("Invalid command. Call 'help' to see available commands.\n")
		}
	}
	return nil
}
