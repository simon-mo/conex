FROM busybox
# FROM rayproject/ray
RUN fallocate -l 30M layer-1 && shred -n 1 layer-1
RUN fallocate -l 30M layer-2 && shred -n 1 layer-2
RUN fallocate -l 30M layer-3 && shred -n 1 layer-3