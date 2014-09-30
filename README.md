gohfile
=======
    go install github.com/paperstreet/gohfile && $GOPATH/bin/gohfile --config=sample.json
    python -c "import urllib2; print(urllib2.urlopen('http://localhost:4000/get/sample', data=b'\x18\x02\x61\x62\x00').read())"
