import sys
import map_pb2
import time
import google.protobuf.internal.decoder as decoder
import google.protobuf.internal.encoder as encoder


def map(key, value):
	return (key, value+" key was "+key)

f = open('workfile', 'w')
f.write("osterhase deluxe\nProtobuf input:\n");

sin = sys.stdin
buf = ''

while True:
	# overwrite buffer
	buf = sin.read(16);
	f.write(buf)
	(size, position) = decoder._DecodeVarint(buf, 0)
	#print("size: "+str(size)+" position: "+str(position))
	if size == 4294967295: # this is a hack my friend we probably need fixlength types for the length
		break;
	toRead = size+position-16;
	buf += sin.read(toRead) # this is probably inefficient because the buffer sizes changes all the time
	#print("bufSize "+str(len(buf)))
	kv = map_pb2.KeyValue()
	kv.ParseFromString(buf[position:position+size])
	#print("key "+kv.key)
	#print("value "+kv.value)
	(rk, rv) = map(kv.key, kv.value)

	retKV = map_pb2.KeyValue()
	retKV.key = rk
	retKV.value = rv
	outBuf = retKV.SerializeToString();
	encoder._EncodeVarint(sys.stdout.write, len(outBuf))
	sys.stdout.write(outBuf);

#print("closing")

f.close();
sin.close();
