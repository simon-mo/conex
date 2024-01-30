FROM busybox
# FROM rayproject/ray
RUN fallocate -l 30M layer-1 && shred -n 1 layer-1
RUN fallocate -l 30M layer-2 && shred -n 1 layer-2
RUN fallocate -l 30M layer-3 && shred -n 1 layer-3
#This makes 3 layers of 30 MB/layer and 1 layer of 397 MB
#Explains why conex takes so long when we did the test months ago