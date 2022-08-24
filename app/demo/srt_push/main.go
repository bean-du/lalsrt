package main

import (
	"flag"
	"fmt"
	"github.com/haivision/srtgo"
	"github.com/q191201771/lal/pkg/base"
	"github.com/q191201771/lal/pkg/logic"
	"github.com/q191201771/lal/pkg/mpegts"
	"github.com/q191201771/naza/pkg/bininfo"
	"github.com/q191201771/naza/pkg/nazalog"
	"log"
	"math"
	"net"
	"os"
)

func main() {
	var (
		customizePubStreamName = "c110"
	)

	confFilename := parseFlag()
	lals := logic.NewLalServer(func(option *logic.Option) {
		option.ConfFilename = confFilename
	})

	session, err := lals.AddCustomizePubSession(customizePubStreamName)
	nazalog.Assert(nil, err)
	// 2. 配置session
	session.WithOption(func(option *base.AvPacketStreamOption) {
		option.VideoFormat = base.AvPacketStreamVideoFormatAnnexb
	})

	options := make(map[string]string)
	options["transtype"] = "live"

	sck := srtgo.NewSrtSocket("0.0.0.0", 6001, options)
	defer sck.Close()

	sck.SetListenCallback(listenCallback)
	if err := sck.Listen(1); err != nil {
		panic(err)
	}

	s, _, err := sck.Accept()
	if err != nil {
		log.Println(err)
	}
	defer s.Close()

	buf := make([]byte, 2048)
	for {
		n, _ := s.Read(buf)
		if n == 0 {
			break
		}

		packets := splitToTs(buf, n)

		for _, p := range packets {
			var pkt base.AvPacket
			ts := mpegts.ParseTsPacketHeader(p[:4])
			fmt.Printf(" pid: %x \n", ts.Pid)
			switch ts.Pid {
			case mpegts.PidPat:
				log.Println("PAT packet")
				start := 4
				if ts.PayloadUnitStart == 1 {
					start = 5
				}
				pat := mpegts.ParsePat(p[start:])
				log.Println(pat.SearchPid(ts.Pid))

			case mpegts.PidAudio:
				log.Println("Audio packet")
				_, n := mpegts.ParsePes(p[4:])
				pkt.PayloadType = base.AvPacketPtAac
				pkt.Payload = p[n+4:]
			case mpegts.PidVideo:
				log.Println("Video packet")
				_, n := mpegts.ParsePes(p[4:])
				pkt.PayloadType = base.AvPacketPtHevc
				pkt.Payload = p[n+4:]

			}
			session.FeedAvPacket(pkt)
		}
	}
}

func listenCallback(socket *srtgo.SrtSocket, version int, addr *net.UDPAddr, streamid string) bool {
	log.Printf("socket will connect, hsVersion: %d, streamid: %s\n", version, streamid)

	// allow connection
	return true
}

func parseFlag() string {
	binInfoFlag := flag.Bool("v", false, "show bin info")
	cf := flag.String("c", "", "specify conf file")
	flag.Parse()

	if *binInfoFlag {
		_, _ = fmt.Fprint(os.Stderr, bininfo.StringifyMultiLine())
		_, _ = fmt.Fprintln(os.Stderr, base.LalFullInfo)
		os.Exit(0)
	}

	return *cf
}

func splitToTs(data []byte, n int) [][]byte {
	var (
		tsLen   = 188
		average = int(math.Ceil(float64(n) / float64(tsLen)))
		res     = make([][]byte, 0, average)
	)
	for i := 1; i <= average; i++ {
		item := make([]byte, 0, 0)

		if i == 1 {
			item = data[0:tsLen]
		} else {
			start := i * tsLen
			end := start + tsLen
			item = data[start:end]
		}

		res = append(res, item)
	}
	return res
}
