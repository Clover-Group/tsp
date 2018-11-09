# Deployment

The prebuilt images are usually available as Docker images on
[Docker Hub](http://hub.docker.com/r/clovergrp/tsp). However, you can
build your own image via SBT, running
```bash
sbt docker:publishLocal
```
to publish the image into your local storage, or
```bash
sbt docker:publish
```
to publish it into some remote server (assumed you have configured
your own setting in `build.sbt`).