package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	ts "github.com/asticode/go-astits"
	"github.com/haivision/srtgo"
	"github.com/q191201771/lal/pkg/aac"
	"github.com/q191201771/lal/pkg/base"
	"github.com/q191201771/lal/pkg/logic"
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

	go func() {
		err := lals.RunLoop()
		nazalog.Infof("server manager done. err=%+v", err)
	}()

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

	reader := bufio.NewReader(s)

	demuxer := ts.NewDemuxer(context.TODO(), reader)
	pkt := base.AvPacket{}

	videoTimestamp := float32(0)
	audioTimestamp := float32(0)
	isSet := false
	for {
		d, err := demuxer.NextData()
		if err != nil {
			if err == ts.ErrNoMorePackets {
				break
			}
			log.Fatalf("%v", err)
		}

		if d.PMT != nil {
			for _, es := range d.PMT.ElementaryStreams {
				switch es.StreamType {
				case ts.StreamTypeH264Video:
					pkt.PayloadType = base.AvPacketPtAvc
				case ts.StreamTypeH265Video:
					pkt.PayloadType = base.AvPacketPtHevc
				case ts.StreamTypeAACAudio:
					pkt.PayloadType = base.AvPacketPtAac
				}
			}
		}

		if d.PES != nil {
			if d.PES.Header.IsVideoStream() {
				videoTimestamp += float32(1000) / float32(15) // 1秒 / fps
				pkt.Timestamp = int64(audioTimestamp)

			} else {
				audioTimestamp += float32(48000*4*2) / float32(8192*2)
				pkt.Timestamp = int64(audioTimestamp)
			}
			pkt.Payload = d.PES.Data
			pkt.Pts = d.PES.Header.OptionalHeader.PTS.Base

			if !isSet && pkt.PayloadType == base.AvPacketPtAac {
				asc, err := aac.MakeAscWithAdtsHeader(pkt.Payload[:aac.AdtsHeaderLength])
				nazalog.Assert(nil, err)
				// 3. 填入aac的audio specific config信息
				session.FeedAudioSpecificConfig(asc)
			}
		}

		if len(pkt.Payload) != 0 {
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
