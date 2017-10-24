import numpy as np
from PIL import Image
import StringIO
import time
import sys

if len(sys.argv) <= 1: 
	IMAGE_FILE = "4k.jpeg"
else:
	IMAGE_FILE = sys.argv[1]

NUM_TRIALS = 100

def encode(arr, format="jpeg"):
	im = Image.fromarray(arr)
	buf = StringIO.StringIO()
	im.save(buf, format=format)
	output = buf.getvalue()
	buf.close()
	return output


def main():
	arr = np.asarray(Image.open(IMAGE_FILE))
	print "Loaded %s with dimensions" % IMAGE_FILE, arr.shape
	
	trials_millis = []
	for _ in xrange(NUM_TRIALS):
		start_time = time.time()
		jpeg_image = encode(arr)
		elapsed_millis = 1000 * (time.time() - start_time)
		trials_millis.append(elapsed_millis)
		print "Encoded jpeg in %dms" % elapsed_millis
	
	avg_millis = np.mean(trials_millis)
	std_millis = np.std(trials_millis)
	print "Ran encoding %d trials with avg=%dms, std=%f" % (
		NUM_TRIALS, avg_millis, std_millis)   
	
	raw_size = len(encode(arr, format="bmp"))
	jpeg_size = len(jpeg_image)
	print "Raw=%d, Jpeg=%d, Ratio=%f" % (
		raw_size, jpeg_size, float(jpeg_size) / raw_size)
	 

if __name__ == "__main__":
	main()




