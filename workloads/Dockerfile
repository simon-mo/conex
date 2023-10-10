FROM busybox
RUN fallocate -l 130M layer-1 && shred -n 1 layer-1
RUN fallocate -l 130M layer-2 && shred -n 1 layer-2
RUN fallocate -l 130M layer-3 && shred -n 1 layer-3