module github.com/filecoin-project/go-sectorbuilder

go 1.13

require (
	github.com/filecoin-project/filecoin-ffi v0.0.0-20200304181354-4446ff8a1bb9
	github.com/filecoin-project/go-address v0.0.2-0.20200218010043-eb9bb40ed5be
	github.com/filecoin-project/go-fil-commcid v0.0.0-20200208005934-2b8bd03caca5
	github.com/filecoin-project/go-paramfetch v0.0.2-0.20200218225740-47c639bab663
	github.com/filecoin-project/specs-actors v0.0.0-20200226200336-94c9b92b2775
	github.com/gopherjs/gopherjs v0.0.0-20190812055157-5d271430af9f // indirect
	github.com/ipfs/go-cid v0.0.5
	github.com/ipfs/go-datastore v0.1.1
	github.com/ipfs/go-ipld-format v0.0.2 // indirect
	github.com/ipfs/go-log v1.0.0
	github.com/ipfs/go-log/v2 v2.0.2
	github.com/mattn/go-isatty v0.0.9 // indirect
	github.com/otiai10/copy v1.0.2
	github.com/pkg/errors v0.9.1
	github.com/polydawn/refmt v0.0.0-20190809202753-05966cbd336a // indirect
	github.com/smartystreets/assertions v1.0.1 // indirect
	github.com/smartystreets/goconvey v0.0.0-20190731233626-505e41936337 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/warpfork/go-wish v0.0.0-20190328234359-8b3e70f8e830 // indirect
	go.opencensus.io v0.22.2
	golang.org/x/crypto v0.0.0-20191206172530-e9b2fee46413 // indirect
	golang.org/x/sys v0.1.0 // indirect
	golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543
)

replace github.com/golangci/golangci-lint => github.com/golangci/golangci-lint v1.18.0

replace github.com/filecoin-project/filecoin-ffi => ./extern/filecoin-ffi
