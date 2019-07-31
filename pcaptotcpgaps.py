#! /usr/bin/env python

import pcapfile.savefile
import numpy
import sys
import re

local = None
if len(sys.argv) > 3:
    with open(sys.argv[3]) as f:
        interfaces = {item[1]: dict(re.findall(r"([a-zA-Z][a-zA-Z ]*)  *([0-9.]*)", item[2], re.M))
                      for item in re.findall(r"(^|\n)([^: \n]*): ((..*\n)*)\n", f.read(), re.M)}
        del interfaces["lo"]
        local = next(iter(interfaces.values()))["inet"].encode("ascii")

with open(sys.argv[1], "rb") as f:
    d = pcapfile.savefile.load_savefile(f, layers=3, verbose=True)

    packets = numpy.zeros(len(d.packets), dtype=[("timestamp", "i8"),
                                                 ("stream", int),
                                                 ("sent", bool),
                                                 ("seqnum", int),

                                                 ("ack", bool),
                                                 ("acknum", int),

                                                 ("src", "<S15"),
                                                 ("dst", "<S15"),
                                                 ("src_port", int),
                                                 ("dst_port", int)])

    streams = {}
    next_streamid = 0

    for idx, packet in enumerate((packet for packet in d.packets
                                  if packet.packet.type == 2048 and packet.packet.payload.p == 6)):            
        src = (packet.packet.payload.src,
               packet.packet.payload.payload.src_port)
        dst = (packet.packet.payload.dst,
               packet.packet.payload.payload.dst_port)
        # print("SRC %s DST %s LOCAL %s => %s" % (repr(src), repr(dst), repr(local), repr(packet.packet.payload.src == local)))
        
        streamname = tuple(sorted((src, dst)))
        if streamname not in streams:
            streams[streamname] = next_streamid
            next_streamid += 1
        streamid = streams[streamname]
        if local is not None:
            sent = packet.packet.payload.src == local
        else:
            sent = src == streamname[0]
        
        packets[idx]["timestamp"] = packet.timestamp * 1e6 + packet.timestamp_us
        packets[idx]["stream"] = streamid
        packets[idx]["sent"] = sent
        packets[idx]["seqnum"] = packet.packet.payload.payload.seqnum
        packets[idx]["ack"] = packet.packet.payload.payload.ack
        packets[idx]["acknum"] = packet.packet.payload.payload.acknum
        packets[idx]["src"] = packet.packet.payload.src
        packets[idx]["src_port"] = packet.packet.payload.payload.src_port
        packets[idx]["dst"] = packet.packet.payload.dst
        packets[idx]["dst_port"] = packet.packet.payload.payload.dst_port
        
    numpy.savez_compressed(sys.argv[2], packets=packets[:idx+1])

