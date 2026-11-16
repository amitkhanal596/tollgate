// Command stall is the benchmark rig's measuring helper. It is NOT part of
// Tollgate; it exists so a benchmark run can tell "the gateway was slow" from
// "the whole Docker VM froze". Three modes, all printing to stdout:
//
//	stall   once a second: "<epoch_s> <max_gap_ms>", where max_gap is the
//	        longest time between two consecutive 1 ms sleeps. A healthy VM
//	        gives 1-3 ms; a VM-wide freeze (host memory pressure, sleep) shows
//	        up as 100 ms to several seconds, for every container at once.
//	net     twice a second, one JSON line with TCP counters of the network
//	        namespace it runs in (start it with --network container:<gateway>):
//	        ActiveOpens from /proc/net/snmp (outgoing connections opened, i.e.
//	        the gateway's upstream dials) and TIME_WAIT sockets from sockstat.
//	time    print the VM's epoch milliseconds and exit (clock offset check).
//
// All timestamps come from the VM clock, the same clock the k6 containers use.
package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	mode := "stall"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	switch mode {
	case "stall":
		runStall()
	case "net":
		runNet()
	case "time":
		fmt.Println(time.Now().UnixMilli())
	default:
		fmt.Fprintln(os.Stderr, "usage: helper stall|net|time")
		os.Exit(2)
	}
}

// stopped is closed on SIGTERM/SIGINT so `docker stop` ends the loops promptly.
func stopped() <-chan os.Signal {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	return c
}

func runStall() {
	stop := stopped()
	last := time.Now()
	sec := last.Unix()
	var maxGap time.Duration
	for {
		select {
		case <-stop:
			return
		default:
		}
		time.Sleep(time.Millisecond)
		now := time.Now()
		if g := now.Sub(last); g > maxGap {
			maxGap = g
		}
		last = now
		if now.Unix() != sec {
			fmt.Printf("%d %.1f\n", sec, float64(maxGap.Microseconds())/1000)
			sec, maxGap = now.Unix(), 0
		}
	}
}

func runNet() {
	stop := stopped()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-tick.C:
			opens, err1 := activeOpens("/proc/net/snmp")
			tw, err2 := timeWait("/proc/net/sockstat")
			if err1 != nil || err2 != nil {
				fmt.Fprintln(os.Stderr, "read error:", err1, err2)
				continue
			}
			fmt.Printf("{\"t_ms\":%d,\"active_opens\":%d,\"time_wait\":%d}\n", now.UnixMilli(), opens, tw)
		}
	}
}

// activeOpens reads Tcp.ActiveOpens: /proc/net/snmp has a "Tcp:" header line
// with field names followed by a "Tcp:" line with the values.
func activeOpens(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	var header []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || fields[0] != "Tcp:" {
			continue
		}
		if header == nil {
			header = fields
			continue
		}
		for i, name := range header {
			if name == "ActiveOpens" && i < len(fields) {
				return strconv.ParseInt(fields[i], 10, 64)
			}
		}
	}
	return 0, fmt.Errorf("ActiveOpens not found in %s", path)
}

// timeWait reads the "tw" count from the "TCP: inuse N orphan N tw N ..." line.
func timeWait(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || fields[0] != "TCP:" {
			continue
		}
		for i := 1; i+1 < len(fields); i++ {
			if fields[i] == "tw" {
				return strconv.ParseInt(fields[i+1], 10, 64)
			}
		}
	}
	return 0, fmt.Errorf("tw not found in %s", path)
}
