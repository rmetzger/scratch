import sys
import map_pb2
import time
import google.protobuf.internal.decoder as decoder

f = open('workfile', 'w')
f.write("osterhase deluxe\nProtobuf input:\n");



sin = sys.stdin
buf = ''
buf = sin.read(16);
f.write(buf)
(size, position) = decoder._DecodeVarint(buf, 0)
print("size: "+str(size)+" position: "+str(position))
toRead = size+position-16;
buf += sin.read(toRead)
print("bufSize "+str(len(buf)))



kv = map_pb2.KeyValue()
kv.ParseFromString(buf[position:position+size])

print("key "+kv.key)

time.sleep(15)

f.close();
sin.close();
