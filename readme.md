dvara [![Build Status](https://secure.travis-ci.org/facebookgo/dvara.png)](http://travis-ci.org/facebookgo/dvara)
=====

dvara provides a connection pooling proxy for
[MongoDB](http://www.mongodb.org/). For more information look at the associated
blog post: http://blog.parse.com/2014/06/23/dvara/.

To build from source you'll need [Go](http://golang.org/). With it you can install it using:

    go get github.com/facebookgo/dvara/cmd/dvara

To start the app, after install it, run:

   dvara -addrs=$HOST:$PORT where host and port is location of mongo db instance

Library documentation: https://godoc.org/github.com/intercom/dvara
