import sys


f = open('workfile', 'w')
f.write("osterhase deluxe\nProtobuf input:\n");



for line in sys.stdin:
	print("line="+line)
	if line == "ENDE!!\n":
		print("Match")
		break
	f.write(line)

f.close();

