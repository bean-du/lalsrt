package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	ts "github.com/asticode/go-astits"
	"github.com/haivision/srtgo"
	"github.com/q191201771/lal/pkg/aac"
	"github.com/q191201771/lal/pkg/avc"
	"github.com/q191201771/lal/pkg/base"
	"github.com/q191201771/lal/pkg/logic"
	"github.com/q191201771/naza/pkg/bininfo"
	"github.com/q191201771/naza/pkg/nazalog"
	"log"
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
	pkts := make(map[uint16]*base.AvPacket)
	pmts := map[uint16]*ts.PMTData{}
	gotAllPMTs := false

	var pat *ts.PATData

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

		if d.PAT != nil {
			pat = d.PAT
			gotAllPMTs = false
			continue
		}

		if d.PMT != nil {
			pmts[d.PMT.ProgramNumber] = d.PMT

			gotAllPMTs = true
			for _, p := range pat.Programs {
				_, ok := pmts[p.ProgramNumber]
				if !ok {
					gotAllPMTs = false
					break
				}
			}

			if !gotAllPMTs {
				continue
			}

			for _, pmt := range pmts {
				for _, es := range pmt.ElementaryStreams {
					_, ok := pkts[es.ElementaryPID]
					if ok {
						continue
					}
					var payloadType base.AvPacketPt
					switch es.StreamType {
					case ts.StreamTypeH264Video:
						payloadType = base.AvPacketPtAvc
					case ts.StreamTypeH265Video:
						payloadType = base.AvPacketPtHevc
					case ts.StreamTypeAACAudio:
						payloadType = base.AvPacketPtAac
					}

					pkts[es.ElementaryPID] = &base.AvPacket{
						PayloadType: payloadType,
					}
				}
			}
		}
		if !gotAllPMTs {
			continue
		}

		if d.PES != nil {

			pid := d.FirstPacket.Header.PID
			pkt, ok := pkts[pid]
			if !ok {
				log.Printf("Got payload for unknown PID %d", pid)
				continue
			}

			if d.PES.Header.IsVideoStream() {
				pkt.Timestamp = int64(videoTimestamp)
				pkt.Payload = d.PES.Data
				pkt.Pts = d.PES.Header.OptionalHeader.PTS.Base

				t := avc.ParseNaluType(d.PES.Data[0])
				if t == avc.NaluTypeSps || t == avc.NaluTypePps || t == avc.NaluTypeSei {
					// noop
				} else {
					videoTimestamp += float32(1000) / float32(25) // 1秒 / fps
				}

			} else {
				pkt.Timestamp = int64(audioTimestamp)
				if d.PES.Header.PacketLength != 0 {
					pkt.Payload = d.PES.Data
					pkt.Pts = d.PES.Header.OptionalHeader.PTS.Base
				}

				audioTimestamp += float32(44100*4*2) / float32(8192*2)
				//audioTimestamp += float32(48000*4*2) / float32(8192*2)
				//audioTimestamp += float32(1024/44100) * 1000

				if !isSet && pkt.PayloadType == base.AvPacketPtAac {
					asc, err := aac.MakeAscWithAdtsHeader(pkt.Payload[:aac.AdtsHeaderLength])
					nazalog.Assert(nil, err)
					session.FeedAudioSpecificConfig(asc)
					isSet = true
				}
			}
			session.FeedAvPacket(*pkt)
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

func mergePackets(audioPackets, videoPackets []base.AvPacket) (packets []base.AvPacket) {
	var i, j int
	for {
		// audio数组为空，将video的剩余数据取出，然后merge结束
		if i == len(audioPackets) {
			packets = append(packets, videoPackets[j:]...)
			break
		}

		//
		if j == len(videoPackets) {
			packets = append(packets, audioPackets[i:]...)
			break
		}

		// 音频和视频都有数据，取时间戳小的
		if audioPackets[i].Timestamp < videoPackets[j].Timestamp {
			packets = append(packets, audioPackets[i])
			i++
		} else {
			packets = append(packets, videoPackets[j])
			j++
		}
	}

	return
}
